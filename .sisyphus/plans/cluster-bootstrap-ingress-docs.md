# Cluster Baseline, ingress-nginx, and Canonical Infra Overview

## TL;DR
> **Summary**: Verify the current Kubernetes baseline with hard stop conditions, install and internally validate `ingress-nginx` as the first restored shared add-on, then create a brand-new canonical infra document at the repo root using historical materials as reference only.
> **Deliverables**:
> - Verified cluster baseline evidence for the current `vmware-home` context
> - Installed `ingress-nginx` controller with internal readiness proof
> - New root `docs/infra_overview.md` written in Markdown + Mermaid
> - Explicit future-update rule that keeps `docs/infra_overview.md` as the canonical infra document
> **Effort**: Short
> **Parallel**: NO
> **Critical Path**: Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 → Task 7 → Task 8

## Context
### Original Request
- Ignore the previous docs as canonical outputs and create new ones.
- Check the current cluster first.
- If the cluster is okay, restore `ingress-nginx` first.
- Create a new root-level `docs/` directory and a new explanation file.
- Visualize the infra layer in that document.
- Keep updating that same document in later infrastructure requests.

### Interview Summary
- Historical materials under `history/` are reference-only inputs, not the new source of truth.
- Live cluster access works from this environment.
- Current live baseline already has four `Ready` nodes, healthy Calico in `calico-system`, and `metrics-server` installed.
- `ingress-nginx` is not installed yet.
- `metallb-system` is not installed yet.
- `SparkApplication` CRDs are absent, but that is out of scope for this slice.
- The repo root currently contains only `history/`, so the new root `docs/` tree will be created from scratch.

### Metis Review (gaps addressed)
- Baseline health, ingress controller readiness, and external exposure are treated as separate gates.
- This slice does **not** expand into MetalLB, DNS, Spark, Airflow, Trino, Grafana, or full platform recovery.
- `ingress-nginx` success is defined as controller installation + internal readiness, not public network reachability.
- The new canonical doc target is fixed now as `docs/infra_overview.md`.
- The document must state that `history/` is reference-only and that future infra changes update this file.

## Work Objectives
### Core Objective
Produce a safe, evidence-driven first infra reconstruction slice that validates the cluster baseline, restores `ingress-nginx` as the first shared add-on, and establishes a new canonical root-level infra overview document.

### Deliverables
- Cluster baseline check results captured in `.sisyphus/evidence/`
- `ingress-nginx` installed in namespace `ingress-nginx`
- Internal controller-health evidence captured in `.sisyphus/evidence/`
- Root `docs/` directory
- `docs/infra_overview.md`
- A documented update rule inside `docs/infra_overview.md` for future infrastructure slices

### Definition of Done (verifiable conditions with commands)
- `kubectl config current-context` returns `vmware-home`
- `kubectl get nodes -o wide` shows all expected nodes `Ready`
- `kubectl get pods -n calico-system` shows Calico components `Running`
- `kubectl wait -n ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=180s` exits `0`
- `kubectl get ingressclass nginx` returns a controller-managed ingress class named `nginx`
- `test -d docs && test -f docs/infra_overview.md` exits `0`
- `python - <<'PY'
from pathlib import Path
text = Path('docs/infra_overview.md').read_text()
assert 'Canonical infrastructure overview' in text
assert 'Update this file for future infrastructure changes' in text
assert '```mermaid' in text
PY` exits `0`

### Must Have
- Hard stop conditions before any mutating cluster command
- `ingress-nginx` installed via Helm into `ingress-nginx`
- `ingressClassName/controller` standardized on `nginx`
- Internal ingress-controller health proof independent of MetalLB
- New canonical document under root `docs/`
- Mermaid-based infra visualization
- Explicit canonical-source statement inside the new doc
- Explicit future-update workflow inside the new doc

### Must NOT Have (guardrails, AI slop patterns, scope boundaries)
- No MetalLB installation in this slice
- No DNS overhaul in this slice
- No Airflow, Trino, OpenLineage, Grafana, Spark Operator, or application deployment in this slice
- No copying old docs verbatim into the new canonical doc
- No treating historical logs as proof of current runtime state
- No success criteria based only on `EXTERNAL-IP` or public ingress reachability
- No repo cleanup outside the new root `docs/` directory

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: none — cluster verification and document verification are command-driven
- QA policy: Every task includes agent-executed scenarios with explicit stop conditions
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`

## Execution Strategy
### Parallel Execution Waves
> This slice is intentionally sequential because each later step depends on verified runtime state from the prior step.

Wave 1: Tasks 1-5 — cluster gate, pre-install audit, ingress install, readiness verification, internal smoke test

Wave 2: Tasks 6-8 — create root docs tree, write canonical infra overview, finalize ongoing update workflow

### Dependency Matrix (full, all tasks)
| Task | Depends On | Blocks |
|---|---|---|
| 1 | None | 2, 3, 4, 5, 6, 7, 8 |
| 2 | 1 | 3, 4, 5 |
| 3 | 2 | 4, 5 |
| 4 | 3 | 5, 7 |
| 5 | 4 | 7 |
| 6 | 1 | 7, 8 |
| 7 | 4, 5, 6 | 8 |
| 8 | 7 | F1, F2, F3, F4 |
| F1 | 1-8 | completion |
| F2 | 1-8 | completion |
| F3 | 1-8 | completion |
| F4 | 1-8 | completion |

### Agent Dispatch Summary (wave → task count → categories)
| Wave | Task Count | Categories |
|---|---:|---|
| 1 | 5 | unspecified-high |
| 2 | 3 | writing, unspecified-high |
| Final Verification | 4 | oracle, unspecified-high, deep |

## TODOs
> Implementation + Test = ONE task. Never separate.
> EVERY task MUST have: Agent Profile + Parallelization + QA Scenarios.

- [ ] 1. Run the baseline cluster health gate

  **What to do**: Verify the active cluster context and health before any install step. Run `kubectl config current-context`, `kubectl get nodes -o wide`, `kubectl get pods -n calico-system`, `kubectl get pods -n kube-system`, and `kubectl get events -A --sort-by=.lastTimestamp`. Save the command outputs into `.sisyphus/evidence/task-1-baseline-health.txt`. Continue only if the context is `vmware-home`, all nodes are `Ready`, Calico is healthy, and there are no unresolved critical warnings that block add-on installation.
  **Must NOT do**: Do not install anything. Do not treat missing `ingress-nginx` or missing MetalLB as a baseline failure. Do not ignore `NotReady`, `CrashLoopBackOff`, or Calico instability.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: live-cluster verification with operational stop conditions
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 2, 3, 4, 5, 6, 7, 8 | Blocked By: none

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/docs/runbook_kubernetes_cluster_recovery.md:5-12` - Defines the cluster recovery success gate and prerequisite expectations
  - Pattern: `history/docs/k8s_cluster_recovery_log.md:70-118` - Shows the prior successful recovery evidence to mirror structurally, but not to treat as current truth
  - Pattern: `history/docs/issue/calico_tailscale0_autodetect_issue.md:1-20` - Documents the Calico `tailscale0` autodetect failure mode and exact remediation if baseline health is false

  **Acceptance Criteria** (agent-executable only):
  - [ ] `kubectl config current-context` returns exactly `vmware-home`
  - [ ] `kubectl get nodes -o wide` shows all expected nodes in `Ready`
  - [ ] `kubectl get pods -n calico-system` shows `calico-node` and supporting Calico components `Running`
  - [ ] `kubectl get pods -n kube-system` shows no critical control-plane or metrics pods in `CrashLoopBackOff` or `Error`
  - [ ] `.sisyphus/evidence/task-1-baseline-health.txt` exists and contains the command outputs

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path baseline gate
    Tool: Bash
    Steps: Run the five baseline commands in order; redirect full output into .sisyphus/evidence/task-1-baseline-health.txt
    Expected: All commands exit 0, context is vmware-home, nodes are Ready, Calico is Running, no blocking system failure is present
    Evidence: .sisyphus/evidence/task-1-baseline-health.txt

  Scenario: Failure gate blocks installation
    Tool: Bash
    Steps: Evaluate the saved output for NotReady nodes, failing Calico pods, or blocking kube-system failures; if any are present, append "BLOCKED" and the exact failing command to the same evidence file and stop the plan before Task 2
    Expected: No install command is executed when the baseline gate fails
    Evidence: .sisyphus/evidence/task-1-baseline-health.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 2. Run the pre-install ingress audit

  **What to do**: Check whether any conflicting ingress installation already exists. Run `kubectl get ns ingress-nginx`, `helm list -A`, `kubectl get ingressclass`, `kubectl get validatingwebhookconfigurations`, and `kubectl get svc -A`. Save outputs to `.sisyphus/evidence/task-2-ingress-precheck.txt`. Proceed only if there is no active release collision, no foreign `IngressClass` conflict for `nginx`, and no leftover broken `ingress-nginx` namespace objects that would make `helm upgrade --install` ambiguous.
  **Must NOT do**: Do not delete leftover resources in this task. Do not install `ingress-nginx` if prechecks show a conflicting controller/class ownership problem.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: preflight verification against live-cluster conflicts
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 3, 4, 5 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/docs/runbook_kubernetes_cluster_recovery.md:82-103` - Historical ingress-nginx and MetalLB recovery sequence; use only the ingress portion in this slice
  - Pattern: `history/helm/nginx/airflow-ingress-manual.yaml:1-25` - Existing app ingress expects an nginx-class controller
  - Pattern: `history/helm/nginx/monitoring-ingress-manual.yaml:1-18` - Shows `ingressClassName: nginx` pattern in current historical ingress resources
  - Pattern: `history/infra/trino/k8s/trino-ingress.yaml:1-19` - Shows legacy nginx ingress annotation pattern that the restored controller must support
  - Pattern: `history/infra/openlineage/k8s/30-ingress.yaml:1-19` - Shows another nginx-based ingress dependency

  **Acceptance Criteria** (agent-executable only):
  - [ ] `.sisyphus/evidence/task-2-ingress-precheck.txt` exists
  - [ ] No active Helm release named `ingress-nginx` already owns the target namespace unless it is the intended target for `upgrade`
  - [ ] No conflicting non-target `IngressClass` named `nginx` is present
  - [ ] Precheck output clearly records the state of `ingress-nginx` namespace, Helm releases, ingress classes, webhooks, and cluster services

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path pre-install audit
    Tool: Bash
    Steps: Run the five precheck commands; redirect full output into .sisyphus/evidence/task-2-ingress-precheck.txt
    Expected: No conflicting controller ownership is found and installation can proceed
    Evidence: .sisyphus/evidence/task-2-ingress-precheck.txt

  Scenario: Conflict blocks installation
    Tool: Bash
    Steps: If helm output or ingressclass output shows a conflicting existing controller/class, append "BLOCKED: ingress conflict" plus the exact offending release/class to the evidence file and stop before Task 3
    Expected: Installation does not start while a controller/class conflict is unresolved
    Evidence: .sisyphus/evidence/task-2-ingress-precheck.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 3. Install `ingress-nginx` as the first restored shared add-on

  **What to do**: Install `ingress-nginx` via Helm using explicit controller/class/service settings. Run:
  `helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx`
  `helm repo update`
  `helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx --namespace ingress-nginx --create-namespace --set controller.ingressClassResource.name=nginx --set controller.ingressClass=nginx --set controller.service.type=LoadBalancer`
  After install, save `helm status ingress-nginx -n ingress-nginx` and `helm list -n ingress-nginx` output to `.sisyphus/evidence/task-3-ingress-install.txt`.
  **Must NOT do**: Do not install MetalLB. Do not apply application ingress resources yet. Do not change the service type away from `LoadBalancer` in this slice.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: mutating shared cluster add-on install with exact command discipline
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 4, 5 | Blocked By: 2

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/docs/runbook_kubernetes_cluster_recovery.md:82-90` - Historical ingress-nginx Helm installation sequence
  - Pattern: `history/docs/platform_runbook.md:37-56` - Shows nginx ingress as a shared platform component in front of later services
  - Pattern: `history/helm/nginx/monitoring-ingress-manual.yaml:1-18` - Confirms the intended ingress class name is `nginx`

  **Acceptance Criteria** (agent-executable only):
  - [ ] Helm install/upgrade command exits `0`
  - [ ] Namespace `ingress-nginx` exists after the command
  - [ ] `.sisyphus/evidence/task-3-ingress-install.txt` exists and contains `helm status` output for `ingress-nginx`
  - [ ] The installed controller advertises or creates an `nginx` ingress class

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path ingress installation
    Tool: Bash
    Steps: Run the three Helm commands exactly; save helm status and helm list output to .sisyphus/evidence/task-3-ingress-install.txt
    Expected: Helm reports a deployed or upgraded ingress-nginx release in namespace ingress-nginx
    Evidence: .sisyphus/evidence/task-3-ingress-install.txt

  Scenario: Install failure halts slice
    Tool: Bash
    Steps: If the Helm install exits non-zero, save the failing command output to the same evidence file and stop before Task 4
    Expected: No later readiness or docs tasks run on a failed ingress install
    Evidence: .sisyphus/evidence/task-3-ingress-install.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 4. Verify `ingress-nginx` controller readiness without depending on MetalLB

  **What to do**: Verify the installed controller is operational internally. Run `kubectl get pods -n ingress-nginx`, `kubectl wait -n ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=180s`, `kubectl get svc -n ingress-nginx`, `kubectl get endpoints -n ingress-nginx`, `kubectl get ingressclass nginx`, `kubectl get jobs -n ingress-nginx`, and `kubectl get events -n ingress-nginx --sort-by=.lastTimestamp`. Save all outputs to `.sisyphus/evidence/task-4-ingress-readiness.txt`.
  **Must NOT do**: Do not fail the task only because the controller service does not yet have a public `EXTERNAL-IP`. Do not proceed if controller pods are not Ready or controller endpoints are empty.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: readiness verification for a live cluster add-on
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 5, 7 | Blocked By: 3

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/helm/nginx/monitoring-ingress-manual.yaml:1-18` - Confirms the expected ingress class name is `nginx`
  - Pattern: `history/infra/trino/k8s/trino-ingress.yaml:6-10` - Confirms later consumers assume an nginx ingress controller exists
  - Pattern: `history/infra/openlineage/k8s/30-ingress.yaml:6-10` - Confirms additional later consumers assume an nginx ingress controller exists

  **Acceptance Criteria** (agent-executable only):
  - [ ] At least one controller pod in `ingress-nginx` becomes `Ready`
  - [ ] `kubectl wait` on the controller selector exits `0`
  - [ ] `kubectl get endpoints -n ingress-nginx ingress-nginx-controller` returns at least one endpoint address
  - [ ] `kubectl get ingressclass nginx` succeeds
  - [ ] `.sisyphus/evidence/task-4-ingress-readiness.txt` exists with pod/service/endpoints/job/event outputs

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path controller readiness
    Tool: Bash
    Steps: Run the seven readiness commands in order; redirect output to .sisyphus/evidence/task-4-ingress-readiness.txt
    Expected: Controller pod becomes Ready, endpoints are populated, ingress class nginx exists, and no admission job failure blocks the controller
    Evidence: .sisyphus/evidence/task-4-ingress-readiness.txt

  Scenario: Readiness failure stops further cluster work
    Tool: Bash
    Steps: If kubectl wait fails, append describe/events output for the controller pods or jobs to the same evidence file and stop before Task 5
    Expected: The plan halts on internal controller failure instead of proceeding to smoke tests
    Evidence: .sisyphus/evidence/task-4-ingress-readiness.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 5. Run an internal smoke test against the controller health endpoint

  **What to do**: Verify the restored controller responds on its internal health endpoint without requiring public networking. Determine the first controller pod name with `kubectl get pod -n ingress-nginx -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].metadata.name}'`, port-forward `10254:10254` from that pod, then run `curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:10254/healthz`. Save the exact commands and outputs to `.sisyphus/evidence/task-5-ingress-smoke.txt`.
  **Must NOT do**: Do not use public DNS or LoadBalancer IP checks in this slice. Do not skip cleanup of the background port-forward process.

  **Recommended Agent Profile**:
  - Category: `unspecified-high` - Reason: live runtime verification with exact command expectations
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: 7 | Blocked By: 4

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/docs/platform_runbook.md:23-35` - Historical external endpoints prove later traffic is ingress-based, but this slice intentionally uses an internal verification path first
  - Pattern: `history/docs/runbook_kubernetes_cluster_recovery.md:145-153` - Historical runbook verifies ingress behavior with in-cluster curl patterns after controller setup

  **Acceptance Criteria** (agent-executable only):
  - [ ] Port-forward to the controller pod starts successfully
  - [ ] `curl` to `http://127.0.0.1:10254/healthz` returns `200`
  - [ ] Background port-forward process is terminated cleanly
  - [ ] `.sisyphus/evidence/task-5-ingress-smoke.txt` exists with the pod name, command transcript, and HTTP status

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path internal smoke test
    Tool: Bash
    Steps: Resolve the first controller pod name; start kubectl port-forward on 10254; curl /healthz; stop the port-forward; save all output to .sisyphus/evidence/task-5-ingress-smoke.txt
    Expected: HTTP status is 200 and the controller is internally healthy
    Evidence: .sisyphus/evidence/task-5-ingress-smoke.txt

  Scenario: Smoke test failure captures blocker details
    Tool: Bash
    Steps: If port-forward or curl fails, append controller pod logs or describe output to the same evidence file and stop before Task 6
    Expected: Failure is recorded with concrete output and no claim of success is made
    Evidence: .sisyphus/evidence/task-5-ingress-smoke.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 6. Create the new root `docs/` tree and canonical doc skeleton

  **What to do**: Create a brand-new root `docs/` directory and add `docs/infra_overview.md`. Write the initial skeleton with the exact sections: title, canonical-source statement, current validated cluster state, restored shared add-ons, not-yet-restored shared add-ons, external dependencies, Mermaid diagrams, historical references, and update workflow. Include the literal strings `Canonical infrastructure overview` and `Update this file for future infrastructure changes`.
  **Must NOT do**: Do not modify anything inside `history/`. Do not copy old docs verbatim. Do not create additional root docs files in this slice.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: this is a new canonical documentation artifact
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: 7, 8 | Blocked By: 1

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/docs/architecture_visuals.md:1-117` - Established Mermaid + Markdown visualization style
  - Pattern: `history/README.md:43-58` - Historical distinction between active paths and history-only paths
  - Pattern: `history/docs/platform_runbook.md:5-18` - Canonical path framing for platform layers, useful as reference only

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docs/` exists at repo root
  - [ ] `docs/infra_overview.md` exists
  - [ ] The file contains `Canonical infrastructure overview`
  - [ ] The file contains `Update this file for future infrastructure changes`
  - [ ] The file contains at least one `` ```mermaid `` block marker

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path docs skeleton creation
    Tool: Bash
    Steps: Create docs/ and docs/infra_overview.md; run test -d docs; run test -f docs/infra_overview.md; grep for the required canonical/update strings and mermaid marker
    Expected: The new canonical doc exists at the root and includes the required anchor strings
    Evidence: .sisyphus/evidence/task-6-doc-skeleton.txt

  Scenario: Failure path prevents partial docs claim
    Tool: Bash
    Steps: If any file/directory check fails, append the failing command and current tree snapshot to .sisyphus/evidence/task-6-doc-skeleton.txt and stop before Task 7
    Expected: No later doc-finalization step runs on a missing or malformed skeleton
    Evidence: .sisyphus/evidence/task-6-doc-skeleton.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 7. Author the validated infra overview and Mermaid diagrams

  **What to do**: Fill `docs/infra_overview.md` with only validated or explicitly-labeled information. Include: the current cluster context (`vmware-home`), the current validated node inventory, current status of Calico and metrics-server, the fact that `ingress-nginx` has been restored in this slice, the fact that MetalLB is not yet restored, and a clear boundary showing external dependencies such as MinIO, Hive Metastore, and Superset. Add at least two Mermaid diagrams: one for the current cluster bootstrap/shared-add-on layer, and one for the dependency path from ingress to upcoming platform services that are still out of scope in this slice.
  **Must NOT do**: Do not present MetalLB, Airflow, Trino, Grafana, OpenLineage, or Spark as already restored. Do not include unvalidated public IPs as current truth. Do not refer readers back to `history/` as the canonical place to update.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: validated technical documentation with diagrams
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: 8 | Blocked By: 4, 5, 6

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/docs/architecture_visuals.md:7-47` - End-to-end Mermaid flowchart pattern
  - Pattern: `history/docs/architecture_visuals.md:94-117` - Folder/layer map visualization pattern
  - Pattern: `history/docs/platform_runbook.md:23-35` - Historical endpoint/dependency inventory; treat as historical reference only
  - Pattern: `history/docs/runbook_kubernetes_cluster_recovery.md:82-103` - Historical shared add-on order; only ingress portion is in scope now
  - Pattern: `history/infra/trino/k8s/trino-ingress.yaml:1-19` - Future platform service dependency on nginx ingress
  - Pattern: `history/infra/openlineage/k8s/30-ingress.yaml:1-19` - Future platform service dependency on nginx ingress

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docs/infra_overview.md` contains `vmware-home`
  - [ ] `docs/infra_overview.md` contains `ingress-nginx`
  - [ ] `docs/infra_overview.md` contains `MetalLB: not yet restored`
  - [ ] `docs/infra_overview.md` contains at least two `` ```mermaid `` blocks
  - [ ] The doc explicitly labels `history/` as reference-only or non-canonical

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path canonical infra overview
    Tool: Bash
    Steps: Run grep checks for vmware-home, ingress-nginx, MetalLB: not yet restored, reference-only wording, and count mermaid block markers in docs/infra_overview.md
    Expected: The new doc reflects validated current state, clearly labels out-of-scope components, and includes at least two Mermaid diagrams
    Evidence: .sisyphus/evidence/task-7-infra-overview.txt

  Scenario: Failure path rejects unvalidated claims
    Tool: Bash
    Steps: If the doc contains claims that MetalLB or downstream platform services are already restored, append the offending lines to .sisyphus/evidence/task-7-infra-overview.txt and revise before proceeding
    Expected: Only validated or explicitly-labeled future-state information remains in the doc
    Evidence: .sisyphus/evidence/task-7-infra-overview.txt
  ```

  **Commit**: NO | Message: `` | Files: []

- [ ] 8. Finalize the future-update workflow for the canonical doc

  **What to do**: Add a short, explicit maintenance section to `docs/infra_overview.md` that says future infrastructure slices must update this same file rather than writing fresh canonical docs elsewhere. Include what to update after each later slice: validated cluster state, restored/not-yet-restored components, Mermaid diagrams, and external dependency status. Then run a final doc-only diff review to ensure the repo changes in this slice are limited to the new root `docs/` path.
  **Must NOT do**: Do not create a second canonical infra doc. Do not edit `history/`. Do not create a git commit unless the user explicitly requests one in a later turn.

  **Recommended Agent Profile**:
  - Category: `writing` - Reason: documentation ownership and maintenance workflow definition
  - Skills: `[]` - No extra injected skills required
  - Omitted: [`/playwright`] - Not a browser task

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: F1, F2, F3, F4 | Blocked By: 7

  **References** (executor has NO interview context - be exhaustive):
  - Pattern: `history/README.md:43-58` - Historical active-vs-history separation to preserve in the new canonical workflow
  - Pattern: `history/docs/platform_runbook.md:5-18` - Historical canonical layer framing to reflect, but not reuse as the update target

  **Acceptance Criteria** (agent-executable only):
  - [ ] `docs/infra_overview.md` contains `Update this file for future infrastructure changes`
  - [ ] The maintenance section specifies what must be refreshed in later infra slices
  - [ ] `git diff --name-only` is limited to the new root `docs/` path for this slice
  - [ ] No git commit is created unless the user explicitly asks for one later

  **QA Scenarios** (MANDATORY - task incomplete without these):
  ```
  Scenario: Happy path future-update workflow
    Tool: Bash
    Steps: Grep docs/infra_overview.md for the update instruction; run git diff --name-only and verify only docs/ paths changed
    Expected: The canonical update workflow is explicit and the repo diff is constrained to the intended docs scope
    Evidence: .sisyphus/evidence/task-8-update-workflow.txt

  Scenario: Failure path catches scope drift
    Tool: Bash
    Steps: If git diff shows changes outside docs/, append the unexpected paths to .sisyphus/evidence/task-8-update-workflow.txt and remove the drift before final verification
    Expected: Scope drift is blocked before review
    Evidence: .sisyphus/evidence/task-8-update-workflow.txt
  ```

  **Commit**: NO | Message: `` | Files: []

## Final Verification Wave (MANDATORY — after ALL implementation tasks)
> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ playwright if UI)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- Default for this slice: **NO COMMIT**. The user did not request a git commit.
- If the user explicitly requests a commit after execution succeeds, use one atomic docs-only commit:
  - Message: `docs(infra): add canonical ingress bootstrap overview`
  - Files: `docs/infra_overview.md` and the new `docs/` directory only
- Never include cluster-runtime state, historical cleanup, or unrelated repo changes in the commit.

## Success Criteria
- The live cluster baseline is verified with explicit evidence and stop conditions.
- `ingress-nginx` is installed and internally healthy.
- No claim of public ingress reachability is made without MetalLB or a later exposure slice.
- A new canonical root doc exists at `docs/infra_overview.md`.
- The new doc uses Mermaid, reflects only validated current state plus clearly-labeled future dependencies, and names itself as the file to keep updating in later infra requests.
