# Infra

배포/인프라 레이어입니다. 이 repo는 이미 다음 디렉토리에 인프라 아티팩트가 존재합니다.

- Airflow/Ingress Helm: `helm/`
- Spark on K8s (SparkApplication): `infra/spark/k8s/`
- Trino Helm values/Ingress: `infra/trino/k8s/`
- Superset docker-compose: `infra/superset/`

폴더 구조를 표준화하기 위해 `infra/`를 추가했지만,
기존 운영 커맨드/경로 호환성을 위해 실제 파일은 원래 위치에 유지합니다.
