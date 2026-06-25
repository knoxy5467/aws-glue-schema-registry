# Phase 4.15 PBI List — Demo Binary: Narrated Java-Go Cross-Language Cross-Version Interop

## Round: 2

## Overview

6 PBIs total. PBI-01 is the foundation (scaffold + fixtures + narration framework). PBI-02 and PBI-05 can run in parallel once PBI-01 lands (they touch disjoint files: scenario code vs. Makefile/README). PBI-03 depends on PBI-02 (Direction B reuses Direction A's registrations). PBI-04 depends on PBI-03 (serializes all main.go modifications). PBI-06 is the terminal validation gate. Total estimated size: ~1200 LOC of new demo code plus a validation run.

## Topological Order

1. PBI-01 (no deps — foundation)
2. PBI-02, PBI-05 (parallel — both depend only on PBI-01, disjoint files)
3. PBI-03 (depends on PBI-01 + PBI-02)
4. PBI-04 (depends on PBI-01 + PBI-03)
5. PBI-06 (depends on all)

## Summary Table

| ID | Title | Size | Dependencies | Files Touched |
|----|-------|------|--------------|---------------|
| PBI-01 | Scaffold demo binary with narration framework, schema fixtures, and scenario runner | L | none | main.go, narrator.go, scenarios.go (all new) |
| PBI-02 | Implement cross-version Java-to-Go scenarios (Direction A) for all 3 formats | L | PBI-01 | scenarios.go, main.go (modified) |
| PBI-03 | Implement cross-version Go-to-Java scenarios (Direction B) for all 3 formats | M | PBI-01, PBI-02 | scenarios.go, main.go (modified) |
| PBI-04 | Implement summary table, result aggregation, and exit-code logic | S | PBI-01, PBI-03 | main.go, narrator.go (modified) |
| PBI-05 | Add Makefile target and README | S | PBI-01 | Makefile (modified), README.md (new) |
| PBI-06 | End-to-end validation: execute demo against real AWS and capture transcript | S | PBI-01-05 | none (validation only) |

## Isolation Matrix

| PBI | Files | Overlaps With |
|-----|-------|---------------|
| PBI-01 | main.go, narrator.go, scenarios.go | PBI-02, PBI-03, PBI-04 (serialized by dependency chain) |
| PBI-02 | scenarios.go, main.go | PBI-03 (serialized: PBI-03 depends on PBI-02) |
| PBI-03 | scenarios.go, main.go | PBI-02 (serialized), PBI-04 (serialized: PBI-04 depends on PBI-03) |
| PBI-04 | main.go, narrator.go | PBI-03 (serialized: PBI-04 depends on PBI-03) |
| PBI-05 | Makefile, README.md | none (disjoint from all Go source files) |
| PBI-06 | none | none |

All main.go overlaps are serialized by the dependency chain: PBI-01 → PBI-02 → PBI-03 → PBI-04. No parallel execution of PBIs that share files.

## Parallelism Notes

- **Layer 1 (serial):** PBI-01 must complete first (foundation).
- **Layer 2 (parallel):** PBI-02 and PBI-05 can start simultaneously. PBI-02 touches scenario code in scenarios.go + main.go. PBI-05 touches only Makefile + README.md. Zero file overlap.
- **Layer 3 (serial on PBI-02):** PBI-03 waits for PBI-02 (runtime dependency: Direction B reuses Direction A's schema registrations; code dependency: main.go modification ordering).
- **Layer 4 (serial on PBI-03):** PBI-04 waits for PBI-03 (serializes all main.go/narrator.go modifications into a single chain).
- **Layer 5 (serial on all):** PBI-06 runs after everything else merges.
