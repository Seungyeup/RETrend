import streamlit as st
import pandas as pd
import numpy as np
import os
import duckdb
import pydeck as pdk
from datetime import datetime
import plotly.graph_objects as go

# 페이지 설정
st.set_page_config(
    page_title="부동산 3D 지도 시세 대시보드",
    layout="wide"
)
st.title("🏢 부동산 거래 시세 3D 지도 대시보드")

# 데이터 로드 함수
@st.cache_data
def load_data():
    home = os.path.expanduser('~')
    db_path = os.path.join(home, 'realestate.duckdb')
    iceberg_base = os.path.join(home, 'dev/RETrend/tmp/raw/iceberg', 'default')

    con = duckdb.connect(db_path, read_only=True)
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true;")

    df = con.execute(f"""
        SELECT
            t.date,
            t.tradeType,
            t.dealPrice,
            t.floor,
            t.representativeArea,
            t.exclusiveArea,
            t.areaNo,
            t.formattedPrice,
            t.formattedTradeYearMonth,
            c.complexName,
            c.latitude,
            c.longitude,
            c.complexNo
        FROM
            iceberg_scan('{os.path.join(iceberg_base, 'trade_history')}') t
        LEFT JOIN
            iceberg_scan('{os.path.join(iceberg_base, 'complex_info')}') c
        ON t.complexNo = c.complexNo
        ORDER BY t.date DESC
    """).df()

    df['date'] = pd.to_datetime(df['date'])
    return df

def calculate_price_changes(df, start_date, end_date):
    """
    단지+평수별 기간 첫 거래 vs 마지막 거래 기준 변동률 계산
    """
    period_df = df[
        (df['date'].dt.date >= start_date) &
        (df['date'].dt.date <= end_date)
    ].copy()

    if period_df.empty:
        return pd.DataFrame()

    # 각 그룹별 가장 이른 거래
    first_trades = period_df.loc[
        period_df.groupby(['complexNo', 'areaNo'])['date'].idxmin()
    ][['complexNo', 'areaNo', 'dealPrice', 'complexName', 'latitude', 'longitude']]
    first_trades = first_trades.rename(columns={'dealPrice': 'start_price'})

    # 각 그룹별 가장 늦은 거래
    last_trades = period_df.loc[
        period_df.groupby(['complexNo', 'areaNo'])['date'].idxmax()
    ][['complexNo', 'areaNo', 'dealPrice']]
    last_trades = last_trades.rename(columns={'dealPrice': 'end_price'})

    # 병합
    price_changes = pd.merge(
        first_trades,
        last_trades,
        on=['complexNo', 'areaNo'],
        how='inner'
    )

    price_changes['price_change_rate'] = (
        (price_changes['end_price'] - price_changes['start_price']) / price_changes['start_price']
    ) * 100
    price_changes['price_change_amount'] = price_changes['end_price'] - price_changes['start_price']

    mean_change = price_changes['price_change_rate'].mean()
    std_change = price_changes['price_change_rate'].std()
    price_changes['z_score'] = (price_changes['price_change_rate'] - mean_change) / std_change

    def get_performance_grade(z):
        if z >= 1.0:
            return 'excellent'
        elif z >= 0.5:
            return 'good'
        elif z >= -0.5:
            return 'average'
        elif z >= -1.0:
            return 'poor'
        else:
            return 'very_poor'

    price_changes['performance_grade'] = price_changes['z_score'].apply(get_performance_grade)

    return price_changes

def get_color_by_performance(grade):
    color_map = {
        'excellent': [0, 255, 0, 200],
        'good': [144, 238, 144, 200],
        'average': [255, 255, 0, 200],
        'poor': [255, 165, 0, 200],
        'very_poor': [255, 0, 0, 200]
    }
    return color_map.get(grade, [128, 128, 128, 200])

def plot_complex_price_trend_by_area(df, complex_no, complex_name, start_date, end_date, selected_areas):
    complex_data = df[
        (df['complexNo'] == complex_no) &
        (df['areaNo'].isin(selected_areas)) &
        (df['date'].dt.date >= start_date) &
        (df['date'].dt.date <= end_date)
    ].sort_values('date')

    if complex_data.empty:
        return None

    daily_avg = (
        complex_data.groupby(['date', 'areaNo'])
        .agg({'dealPrice': 'mean'})
        .reset_index()
    )

    fig = go.Figure()

    color_palette = [
        "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
        "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52"
    ]

    for i, area in enumerate(sorted(daily_avg['areaNo'].unique())):
        area_data = daily_avg[daily_avg['areaNo'] == area]
        fig.add_trace(go.Scatter(
            x=area_data['date'],
            y=area_data['dealPrice'],
            mode='lines+markers',
            name=f"areaNo {area}",
            line=dict(width=2, color=color_palette[i % len(color_palette)]),
            marker=dict(size=6)
        ))

    fig.update_layout(
        title=f"{complex_name} 평수별 가격 변화 추이 ({start_date} ~ {end_date})",
        xaxis_title="날짜",
        yaxis_title="평균 거래가 (원)",
        hovermode="x unified",
        legend_title="areaNo",
        showlegend=True
    )
    fig.update_yaxes(tickformat=",")
    return fig

# 데이터 준비
try:
    df = load_data()
except Exception as e:
    st.error(f"데이터 로드 중 오류 발생: {e}")
    st.stop()

# 사이드바
st.sidebar.header("📊 필터 설정")
min_date = df['date'].min().date()
max_date = df['date'].max().date()

st.sidebar.subheader("📅 기간 선택")
date_range = st.sidebar.slider(
    "분석 기간",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="YYYY-MM-DD"
)
start_date, end_date = date_range

# 선택 가능한 평수
all_areas = sorted(df['areaNo'].dropna().unique())
selected_areas = st.sidebar.multiselect(
    "비교할 평수를 선택하세요",
    options=all_areas,
    default=all_areas
)

# 가격 변동 계산
price_changes = calculate_price_changes(df, start_date, end_date)
price_changes = price_changes[price_changes['areaNo'].isin(selected_areas)]

if not price_changes.empty:
    st.subheader("📈 가격 변동 통계")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("평균 변동률", f"{price_changes['price_change_rate'].mean():.1f}%")
    with col2:
        st.metric("최대 상승률", f"{price_changes['price_change_rate'].max():.1f}%")
    with col3:
        st.metric("최대 하락률", f"{price_changes['price_change_rate'].min():.1f}%")
    with col4:
        st.metric("분석 대상", f"{len(price_changes):,}개 (단지+평수)")

    # 위치 Offset 계산
    offset_map = {area: i * 0.0002 for i, area in enumerate(sorted(price_changes['areaNo'].unique()))}
    price_changes['longitude_offset'] = price_changes.apply(
        lambda row: row['longitude'] + offset_map.get(row['areaNo'], 0),
        axis=1
    )

    # Tooltip 문자열 생성
    price_changes["tooltip_text"] = (
        "<b>" + price_changes["complexName"] + "</b><br/>"
        + "평수(areaNo): " + price_changes["areaNo"].astype(str) + "<br/>"
        + "변동률: " + price_changes["price_change_rate"].round(1).astype(str) + "%<br/>"
        + "변동금액: " + price_changes["price_change_amount"].round(0).apply(lambda x: f"{x:,.0f}") + "원"
    )

    st.subheader("🗺️ 가격 변동 3D 지도")
    st.markdown("**높이**: 가격 변동률 (%) | **색상**: 상대적 성과")
    legend_data = {
        'excellent': '🟢 매우 좋음 (상위 16%)',
        'good': '🟢 좋음 (상위 31%)',
        'average': '🟡 보통 (중간 38%)',
        'poor': '🟠 나쁨 (하위 31%)',
        'very_poor': '🔴 매우 나쁨 (하위 16%)'
    }
    legend_text = " | ".join([f"{desc}" for desc in legend_data.values()])
    st.markdown(f"**색상 범례**: {legend_text}")

    max_change_rate = abs(price_changes['price_change_rate']).max()
    height_scale = 1000 / max_change_rate if max_change_rate > 0 else 1000
    price_changes['color'] = price_changes['performance_grade'].apply(get_color_by_performance)
    price_changes['elevation'] = abs(price_changes['price_change_rate']) * height_scale

    layer = pdk.Layer(
        "ColumnLayer",
        data=price_changes,
        get_position='[longitude_offset, latitude]',
        get_elevation='elevation',
        elevation_scale=1,
        radius=80,
        get_fill_color='color',
        pickable=True,
        auto_highlight=True,
    )

    midpoint = {
        "latitude": price_changes["latitude"].mean(),
        "longitude": price_changes["longitude"].mean()
    }

    view_state = pdk.ViewState(
        latitude=midpoint["latitude"],
        longitude=midpoint["longitude"],
        zoom=11,
        pitch=45
    )

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "{tooltip_text}",
            "style": {"color": "white"}
        }
    ))

    st.subheader("📊 단지별 상세 분석")
    complex_options = price_changes.sort_values('price_change_rate', ascending=False)

    selected_complex_name = st.selectbox(
        "분석할 단지를 선택하세요:",
        options=complex_options['complexName'].unique(),
        index=0
    )

    selected_complex_info = complex_options[complex_options['complexName'] == selected_complex_name]
    if len(selected_complex_info) > 0:
        selected_complex_no = selected_complex_info.iloc[0]['complexNo']
        price_fig = plot_complex_price_trend_by_area(df, selected_complex_no, selected_complex_name, start_date, end_date, selected_areas)
        if price_fig:
            st.plotly_chart(price_fig, use_container_width=True)
        else:
            st.warning("해당 기간에 거래 데이터가 없습니다.")

    st.subheader("🏆 성과별 평수별 순위")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**📈 상위 10개 (단지+평수)**")
        st.dataframe(
            price_changes.nlargest(10, 'price_change_rate')[['complexName', 'areaNo', 'price_change_rate', 'price_change_amount', 'performance_grade']],
            use_container_width=True
        )
    with col2:
        st.markdown("**📉 하위 10개 (단지+평수)**")
        st.dataframe(
            price_changes.nsmallest(10, 'price_change_rate')[['complexName', 'areaNo', 'price_change_rate', 'price_change_amount', 'performance_grade']],
            use_container_width=True
        )

else:
    st.warning(f"선택한 기간 ({start_date} ~ {end_date})에 거래 데이터가 충분하지 않습니다.")
    st.info("💡 팁: 더 넓은 기간을 선택하거나 다른 날짜 범위를 시도해보세요.")

st.markdown("---")
st.markdown("""
### 📋 사용법
1. **시작일/종료일**을 선택하세요.
2. **비교할 평수**를 체크박스로 선택하세요.
3. **높이**는 시작-종료 거래 기준 가격 변동률입니다.
4. **색상**은 상대적 성과를 나타냅니다.
5. **마우스 호버**로 상세 정보를 확인할 수 있습니다.
""")
