# Kubernetes Cluster 복구 Runbook (etcd 손상/reset 기준)

이 문서는 RETrend 운영 클러스터에서 control plane(etcd 포함) 손상 이후, kubeadm 기반 클러스터를 재구성하는 표준 절차를 정리합니다.

## 범위와 성공 기준

- 범위: 클러스터 자체 복구(control plane + worker 재조인 + 네트워크 애드온 복원)
- 성공 기준:
  - `kubectl get nodes` 결과가 모든 노드 `Ready`
  - `kube-system`의 핵심 파드(CoreDNS/kube-proxy/CNI)가 `Running`
  - `ingress-nginx`, `metallb-system` 파드가 `Running`

## 1) Control Plane 재생성 (control-plane 노드에서 실행)

```bash
sudo kubeadm reset -f
sudo systemctl stop kubelet
sudo rm -rf /etc/kubernetes
sudo rm -rf /var/lib/etcd
sudo rm -rf /etc/cni/net.d
sudo rm -rf /var/lib/cni
sudo systemctl restart containerd
sudo systemctl restart kubelet

sudo kubeadm init \
  --apiserver-advertise-address=<CONTROL_PLANE_IP> \
  --pod-network-cidr=192.168.0.0/16
```

```bash
mkdir -p $HOME/.kube
sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
```

## 2) Worker 재조인

Control Plane에서 join 명령 생성:

```bash
kubeadm token create --print-join-command
```

각 Worker에서 실행:

```bash
sudo kubeadm reset -f
sudo rm -rf /etc/cni/net.d
sudo rm -rf /var/lib/cni
sudo systemctl restart containerd
sudo systemctl restart kubelet
# 위에서 생성된 kubeadm join ... 명령 실행
```

## 3) 네트워크 애드온 복구 (순서 고정)

### 3-0. (단일 노드 클러스터일 때) control-plane taint 해제

worker가 아직 없는 단일 노드 상태에서 애드온 파드가 `Pending`이면 아래를 먼저 실행합니다.

```bash
kubectl taint nodes --all node-role.kubernetes.io/control-plane-
```

> 참고: worker 조인 이후 control-plane 전용 스케줄링을 복원하려면 taint를 다시 설정할 수 있습니다.

### 3-1. Calico 설치

Calico는 공식 설치 문서에서 현재 stable 매니페스트 버전을 확인해 적용합니다.

```bash
kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/<CALICO_TAG>/manifests/calico.yaml
```

멀티 NIC 환경(예: `tailscale0` 존재)에서 Calico가 잘못된 인터페이스 IP를 잡아 BGP 피어링이 실패하면, 아래를 적용해 autodetect 대상을 제한합니다.

```bash
kubectl set env daemonset/calico-node -n kube-system IP_AUTODETECTION_METHOD=skip-interface=tailscale0
kubectl -n kube-system rollout status daemonset/calico-node --timeout=600s
```

### 3-2. ingress-nginx controller 설치

```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace
```

### 3-3. MetalLB 설치 + RETrend 설정 반영

```bash
kubectl create namespace metallb-system --dry-run=client -o yaml | kubectl apply -f -

helm repo add metallb https://metallb.github.io/metallb
helm repo update
helm upgrade --install metallb metallb/metallb \
  --namespace metallb-system

kubectl apply -f helm/metallb/metallb-config.yaml
```

`helm/metallb/metallb-config.yaml`의 주소 풀:

- `172.30.1.240-172.30.1.250`

## 4) 복구 검증 (완료 판정)

```bash
kubectl get nodes -o wide
kubectl get pods -n kube-system
kubectl get pods -n ingress-nginx
kubectl get pods -n metallb-system
```

필요 시 ingress 리소스 확인:

```bash
kubectl get ingress -A
```

## 5) 이 레포 기준 후속 재배포 포인트

클러스터 자체 복구 완료 후, RETrend 리소스는 아래 순서로 재적용합니다.

1. Airflow: `helm/airflow/airflow-onprem.yaml`
2. Ingress 리소스: `helm/nginx/*.yaml`, `infra/*/k8s/*ingress*.yaml`
3. Spark/Trino/OpenLineage: `infra/spark/k8s/*.yaml`, `infra/trino/k8s/*.yaml`, `infra/openlineage/k8s/*.yaml`
4. Grafana: `helm/grafana/values-onprem.yaml` + `helm/nginx/monitoring-ingress-manual.yaml`

Grafana 재배포 명령:

```bash
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm upgrade --install kube-prometheus-stack grafana/grafana \
  --namespace default \
  -f helm/grafana/values-onprem.yaml

kubectl apply -f helm/nginx/monitoring-ingress-manual.yaml
```

복구 검증:

```bash
kubectl get svc -n default kube-prometheus-stack-grafana
kubectl get endpoints -n default kube-prometheus-stack-grafana
kubectl describe ingress -n default monitoring-grafana
kubectl run -n default ingress-curl --rm -i --restart=Never --image=curlimages/curl:8.8.0 -- \
  curl -s -o /dev/null -w "%{http_code}\\n" -H "Host: monitoring.home.lab" \
  http://ingress-nginx-controller.ingress-nginx.svc.cluster.local/login
```
