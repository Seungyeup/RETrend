# Kubernetes Cluster 복구 실행 기록

이 문서는 실제 복구 실행 결과를 남기는 운영 로그입니다.

## 기록 템플릿

- Run ID:
- 실행 일시(KST):
- 실행자:
- 대상 Control Plane IP:
- 결과: `SUCCESS | PARTIAL | BLOCKED`

### 성공 판정 체크

- [ ] `kubectl get nodes` 모든 노드 `Ready`
- [ ] `kubectl get pods -n kube-system` 핵심 파드 `Running`
- [ ] `kubectl get pods -n ingress-nginx` 파드 `Running`
- [ ] `kubectl get pods -n metallb-system` 파드 `Running`

### 실행 로그 (핵심 출력)

```text
<여기에 실제 명령 출력 저장>
```

---

## Run 2026-04-08-01 (사전 점검)

- 실행 일시(KST): 2026-04-08
- 실행자: assistant session
- 결과: `BLOCKED`

### 사전 점검 출력

```text
$ which kubectl
/opt/homebrew/bin/kubectl

$ kubectl config current-context
kubernetes-admin@kubernetes

$ kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'
https://172.30.1.37:6443

$ kubectl cluster-info
The connection to the server 172.30.1.37:6443 was refused - did you specify the right host or port?

$ which kubeadm
kubeadm not found

$ ssh -o BatchMode=yes -o ConnectTimeout=5 172.30.1.37 hostname
Permission denied (publickey,password).
```

### 메모

- 현재 실행 환경(로컬 macOS)에서는 kubeadm이 없고, control-plane SSH 권한도 없어 직접 복구 실행이 불가했습니다.
- 실제 복구는 control-plane/worker 노드에서 `docs/runbook_kubernetes_cluster_recovery.md` 절차대로 수행해야 합니다.

---

## Run 2026-04-08-02 (실복구)

- 실행 일시(KST): 2026-04-08
- 실행자: assistant session (SSH user: dave)
- 대상 Control Plane IP: 172.30.1.37
- 결과: `SUCCESS`

### 성공 판정 체크

- [x] `kubectl get nodes` 모든 노드 `Ready`
- [x] `kubectl get pods -n kube-system` 핵심 파드 `Running`
- [x] `kubectl get pods -n ingress-nginx` 파드 `Running`
- [x] `kubectl get pods -n metallb-system` 파드 `Running`

### 실행 로그 (핵심 출력)

```text
$ sudo kubeadm init --apiserver-advertise-address=172.30.1.37 --pod-network-cidr=192.168.0.0/16
Your Kubernetes control-plane has initialized successfully!

$ kubectl get nodes -o wide
NAME         STATUS   ROLES           AGE     VERSION   INTERNAL-IP   CONTAINER-RUNTIME
k8s-master   Ready    control-plane   3m47s   v1.33.3   172.30.1.37   containerd://1.7.28

$ kubectl get pods -n kube-system -o wide
calico-kube-controllers ... 1/1 Running
calico-node ... 1/1 Running
coredns ... 1/1 Running
etcd-k8s-master ... 1/1 Running
kube-apiserver-k8s-master ... 1/1 Running
kube-controller-manager-k8s-master ... 1/1 Running
kube-proxy ... 1/1 Running
kube-scheduler-k8s-master ... 1/1 Running

$ kubectl get pods -n ingress-nginx -o wide
ingress-nginx-controller-... 1/1 Running

$ kubectl get pods -n metallb-system -o wide
controller-... 1/1 Running
speaker-... 1/1 Running

$ kubectl get ipaddresspool -n metallb-system
first-pool ... ["172.30.1.240-172.30.1.250"]

$ kubectl get l2advertisement -n metallb-system
example ... ["first-pool"]

$ kubeadm token create --print-join-command
kubeadm join 172.30.1.37:6443 --token <redacted> --discovery-token-ca-cert-hash sha256:b833...abd9c
```

### 메모

- 단일 노드 복구 상황이라 `control-plane` taint를 제거한 뒤 ingress-nginx/MetalLB 파드가 스케줄되어 정상화됨.
- worker 노드는 위 join 명령으로 순차 재조인하면 됨.

---

## Run 2026-04-08-03 (worker 3대 재조인)

- 실행 일시(KST): 2026-04-08
- 실행자: assistant session (SSH user: dave)
- 대상 Worker IP: 172.30.1.88, 172.30.1.98, 172.30.1.90
- 결과: `SUCCESS`

### 성공 판정 체크

- [x] worker1/2/3 `kubeadm join` 성공
- [x] `kubectl get nodes -o wide` 4노드 `Ready`
- [x] `kube-system` Calico/CoreDNS/kube-proxy 정상
- [x] `ingress-nginx`, `metallb-system` 파드 정상

### 실행 로그 (핵심 출력)

```text
$ sudo kubeadm join 172.30.1.37:6443 --token <redacted> --discovery-token-ca-cert-hash sha256:b833...abd9c
This node has joined the cluster:
* Certificate signing request was sent to apiserver and a response was received.

$ kubectl get nodes -o wide
k8s-master    Ready  control-plane  172.30.1.37
k8s-worker1   Ready  <none>         172.30.1.88
k8s-worker2   Ready  <none>         172.30.1.98
k8s-worker3   Ready  <none>         172.30.1.90

$ kubectl get pods -n kube-system -o wide
calico-node (master/worker1/worker2/worker3) 1/1 Running
coredns 1/1 Running
kube-proxy (all nodes) 1/1 Running

$ kubectl get pods -n ingress-nginx -o wide
ingress-nginx-controller 1/1 Running

$ kubectl get pods -n metallb-system -o wide
controller 1/1 Running
speaker (master/worker1/worker2/worker3) 1/1 Running
```

### 메모

- master 노드에서 Calico가 `tailscale0` IP를 autodetect 하며 BGP 피어링이 불안정해져, 아래 적용 후 정상화함.
  - `kubectl set env daemonset/calico-node -n kube-system IP_AUTODETECTION_METHOD=skip-interface=tailscale0`
- 적용 후 `kubectl -n kube-system rollout status daemonset/calico-node` 성공 확인.
