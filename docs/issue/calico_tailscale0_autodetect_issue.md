# Calico master autodetect 이슈 기록

## 1. 현상

worker 3대 재조인 이후 `kube-system`에서 master의 Calico 파드가 `Ready`가 되지 않았고, readiness 이벤트에 `BIRD is not ready` 및 `BGP not established with 172.30.1.88,172.30.1.98,172.30.1.90`가 반복되었습니다.

## 2. 원인

master 노드에서 Calico가 노드 IP 자동 감지 시 `tailscale0` 인터페이스의 `100.105.134.40/32`를 선택하여, 클러스터 내부 통신 인터페이스(`172.30.1.37`) 기준 BGP 피어링과 불일치가 발생했습니다.

## 3. 해결방법

Calico DaemonSet에 아래 환경변수를 적용해 `tailscale0`을 autodetect 대상에서 제외하고 롤아웃을 완료했습니다.

```bash
kubectl set env daemonset/calico-node -n kube-system IP_AUTODETECTION_METHOD=skip-interface=tailscale0
kubectl -n kube-system rollout status daemonset/calico-node --timeout=600s
```

적용 후 `calico-node` DaemonSet이 정상 롤아웃 되었고, master/worker 전체 Calico 파드가 `1/1 Running`으로 복구되었습니다.
