helm repo add trino https://trinodb.github.io/charts
helm repo update

kubectl create namespace trino

helm install trino trino/trino \
  -n trino \
  -f ./infra/trino/k8s/values-trino.yaml

helm upgrade --install trino trino/trino \
  -n trino \
  -f ./infra/trino/k8s/values-trino.yaml

kubectl apply -f infra/trino/k8s/trino-ingress.yaml
