# 00_infra

This directory will be rebuilt step by step.

The archived implementation in `history/` is reference-only. New infrastructure work should land here in small, reviewable slices instead of being copied all at once.

## Current sequence

1. Repository hygiene and root workspace setup
2. Cluster bootstrap verification
3. `ingress-nginx` restoration
4. `MetalLB` restoration for external IP assignment
5. Remaining shared infra after the earlier steps are verified
