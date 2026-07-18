# KEDA (standalone deployment)

A production wrapper around the upstream [`kedacore/keda`](https://github.com/kedacore/charts/tree/main/keda)
chart. Deploy this **once per cluster**, separately from the app — the
`asr-gateway` chart's `ScaledObject` (when `autoscaling.mode: keda`) requires the
KEDA operator to already be running.

## What it deploys

The full KEDA stack: the operator, the metrics API server, the admission
webhooks, and the CRDs — with production defaults layered on top of upstream:

- **HA**: 2 replicas each for operator (leader-elected), metrics server, and webhooks
- **PodDisruptionBudgets**: `minAvailable: 1` for each component
- **Pod anti-affinity**: spreads replicas across nodes
- Resource requests/limits set explicitly

## Install

The upstream chart is vendored under `charts/` (via `helm dependency build`), so
this deploys without needing the KEDA repo at install time:

```bash
helm upgrade --install keda deploy/helm/keda \
  --namespace keda --create-namespace
```

To refresh the vendored dependency (e.g. after bumping the version in
`Chart.yaml`):

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm dependency update deploy/helm/keda
```

## Verify

```bash
kubectl get pods -n keda
kubectl get crd | grep keda.sh          # scaledobjects, scaledjobs, triggerauthentications, ...
kubectl api-resources | grep external.metrics   # metrics API registered
```

Once running, install/upgrade `asr-gateway` with `autoscaling.enabled=true`
(default `mode: keda`); its `ScaledObject` will be reconciled by this operator.

## Version

Deploys KEDA `2.20.1` (see `appVersion`). Bump the dependency version in
`Chart.yaml` and re-run `helm dependency update` to upgrade.
