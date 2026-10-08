# Experiment results

| Output | Execution | Recorded outcomes | Report |
|---|---|---|---|
| activity | run_20261004_020915_8da9 | passed: 48 | [activity/SUMMARY.md](activity/SUMMARY.md) |
| buffer | run_20261004_045939_c0fa | passed: 8 | [buffer/SUMMARY.md](buffer/SUMMARY.md) |
| Graph 1 | run_20261003_080700_dfd9 | passed: 3 | [figures/SUMMARY.md](figures/SUMMARY.md) |
| Graph 2 | run_20261003_080700_dfd9 | passed: 60 | [figures/SUMMARY.md](figures/SUMMARY.md) |
| Graph 3 | run_20261003_080700_dfd9 | passed: 60 | [figures/SUMMARY.md](figures/SUMMARY.md) |
| Graph 4 | run_20261003_080700_dfd9 | passed: 30 | [figures/SUMMARY.md](figures/SUMMARY.md) |
| Graph 5 | run_20261003_080700_dfd9 | passed: 80 | [figures/SUMMARY.md](figures/SUMMARY.md) |
| Recovery | run_20261003_080700_dfd9 | passed: 12 | [figures/SUMMARY.md](figures/SUMMARY.md) |
| Supporting checks | run_20261003_080700_dfd9 | passed: 24 | [figures/SUMMARY.md](figures/SUMMARY.md) |

activity: 0 recorded errors.
buffer: 0 recorded errors.
Figure phase status: passed.

Graphs 2 and 3 share trials; their outcome records are separate.
Missing values remain unknown. Failed, incomplete, and unattempted records remain visible.

## Key settings

| Phase | Setting | Value |
|---|---|---|
| activity | region | us-west-2 |
| activity | conditions | quick, spaced, distributed, mixed, continuous_1ps_10p, continuous_1ps_20p, continuous_3ps_10p, continuous_3ps_20p, load_1ps_1000p, load_5ps_1000p, load_20ps_10p, load_20ps_1000p |
| activity | repetitions | 3 |
| activity | burst_repetitions | 3 |
| activity | buffer_bytes | 16777216 |
| activity | queue_visibility_seconds | 180 |
| buffer | region | us-west-2 |
| buffer | conditions | load_20ps_10p, load_20ps_1000p |
| buffer | repetitions | 2 |
| buffer | burst_repetitions | 2 |
| buffer | buffer_sizes | 100, 1000 |
| buffer | buffer_bytes | 16777216 |
| buffer | queue_visibility_seconds | 1000 |
| figures | region | us-west-2 |
| figures | sizes | 100, 1000, 5000, 10000 |
| figures | files | 1, 100, 1000 |
| figures | graph4_partitions | 1000 |
| figures | layout_sizes | 100, 1000, 5000, 10000 |
| figures | repetitions | 5 |
| figures | correctness_repetitions | 3 |
| figures | request_source | cloudwatch |
| figures | audit_wait_seconds | 600 |

Full configuration, source identity, and image identity: [provenance.json](provenance.json).
