# Airflow

Airflow DAG 소스는 기본적으로 `dags/`를 엔트리포인트로 사용합니다.

이 repo에서는 폴더 골격을 표준화하기 위해 `pipelines/airflow/`를 두지만,
호환성(운영/배포 경로)을 위해 DAG 파일은 당분간 `dags/`에 유지합니다.

참고:
- DAGs: `dags/`
- Airflow Helm values: `helm/airflow/airflow-onprem.yaml`