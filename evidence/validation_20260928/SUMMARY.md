# AWS pilot report

Author: Ankit Joshi

Run status: **incomplete_or_failed**.


## Fixture layout

| Partition columns | Starting partitions | Occupied prefixes at each depth |
|---|---|---|
| day / tenant | 100 | 1 / 100 |
| dt / site / hour | 100 | 1 / 5 / 100 |
| tenant / day | 100 | 100 / 100 |

Partition count and prefix structure vary together; compare layouts in separate runs.

## Graph outcomes

| Graph | Trial outcomes | Figures |
|---|---|---|
| 1 | 1/1 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_1.png) · [PDF](graph_1.pdf) |
| 2 | 3/3 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_2.png) · [PDF](graph_2.pdf) |
| 3 | 3/3 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_3.png) · [PDF](graph_3.pdf) |
| 4 | 4/4 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_4.png) · [PDF](graph_4.pdf) |
| 5 | 4/4 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_5.png) · [PDF](graph_5.pdf) |

Graph 1 uses repetition 1, selected before running even if it fails. Binary traces are not averaged.
Graphs 2/4/5 show HEAD + GET + LIST totals on logarithmic axes, summed within each complete repetition before taking the median. Missing components make the total unknown.
Zero counts remain in the CSV and statistics; zero counts/ranges are labeled at the axis because logarithmic axes cannot display zero.
Graph 3 uses solid colored lines. Graphs 2–5 show one median point and thin min–max whiskers where repetitions differ. Individual values stay in the CSV; no p95 is estimated.
Graph 3 triggers: RTMSF successful upload response; crawler immediately before StartCrawler; MSCK immediately before StartQueryExecution. These are method-specific response times.
Blank values remain unknown. Observed CloudWatch HEAD/GET/LIST counts; delivery completeness is not guaranteed. Absent counters become zero only when recorded operations account for AllRequests; otherwise they remain blank. A trial pass does not establish exact accounting.

## Table 1. Crash recovery

| Crash case | Expected after recovery | Observed counts | Crash verified / attempted | Replayed + acknowledged / attempted | Passed / attempted |
|---|---|---|---|---|---|
| ADD — before processing | Registered | Registered: 1; absent: 0; unknown: 0 | 1/1 | 1/1 | 1/1 |
| ADD — after registration, before acknowledgement | Registered | Registered: 1; absent: 0; unknown: 0 | 1/1 | 1/1 | 1/1 |
| DROP — before processing | Absent | Registered: 0; absent: 1; unknown: 0 | 1/1 | 1/1 | 1/1 |
| DROP — after removal, before acknowledgement | Absent | Registered: 0; absent: 1; unknown: 0 | 1/1 | 1/1 | 1/1 |

- ADD — before processing: 1/1 attempted; 0 failed; 0 incomplete.
- ADD — after registration, before acknowledgement: 1/1 attempted; 0 failed; 0 incomplete.
- DROP — before processing: 1/1 attempted; 0 failed; 0 incomplete.
- DROP — after removal, before acknowledgement: 1/1 attempted; 0 failed; 0 incomplete.

Observed states are measured after recovery, before the next cycle step or cleanup. Skipped trials are not attempts.

## Supporting checks

| Check | Passed / attempted | Failed | Incomplete | Not attempted |
|---|---|---|---|---|
| short_burst | 1/1 | 0 | 0 | 0 |
| long_burst | 1/1 | 0 | 0 | 0 |
| two_tables | 1/1 | 0 | 0 | 0 |
| retryable_failures | 1/1 | 0 | 0 | 0 |
| retry_exhaustion | 1/1 | 0 | 0 | 0 |
| duplicate_old_events | 0/0 | 0 | 0 | 1 |
| rapid_changes | 0/0 | 0 | 0 | 1 |

Short-burst work from delivered worker telemetry (including separate processing windows):

| Repetition | Observed file events | Observed PMCP attempts | Observed applied catalog changes |
|---|---|---|---|
| 1 | 20 | 1 | 1 |

## Measurement availability

Available / recorded counts exclude blanks from the available sample size. A 0/0 condition has no point records.

| Graph | Method | Layout | Measurement | X | Available / recorded |
|---|---|---|---|---|---|
| 2 | rtmsf |  | s3_head_requests | 100 | 1/1 |
| 2 | rtmsf |  | s3_get_requests | 100 | 1/1 |
| 2 | rtmsf |  | s3_list_requests | 100 | 1/1 |
| 2 | glue_event_crawler |  | s3_head_requests | 100 | 1/1 |
| 2 | glue_event_crawler |  | s3_get_requests | 100 | 1/1 |
| 2 | glue_event_crawler |  | s3_list_requests | 100 | 1/1 |
| 2 | athena_msck |  | s3_head_requests | 100 | 1/1 |
| 2 | athena_msck |  | s3_get_requests | 100 | 1/1 |
| 2 | athena_msck |  | s3_list_requests | 100 | 1/1 |
| 3 | rtmsf |  | visibility_delay_s | 100 | 1/1 |
| 3 | glue_event_crawler |  | visibility_delay_s | 100 | 1/1 |
| 3 | athena_msck |  | visibility_delay_s | 100 | 1/1 |
| 4 | rtmsf |  | s3_head_requests | 1 | 1/1 |
| 4 | rtmsf |  | s3_head_requests | 100 | 1/1 |
| 4 | rtmsf |  | s3_get_requests | 1 | 1/1 |
| 4 | rtmsf |  | s3_get_requests | 100 | 1/1 |
| 4 | rtmsf |  | s3_list_requests | 1 | 1/1 |
| 4 | rtmsf |  | s3_list_requests | 100 | 1/1 |
| 4 | glue_event_crawler |  | s3_head_requests | 1 | 1/1 |
| 4 | glue_event_crawler |  | s3_head_requests | 100 | 1/1 |
| 4 | glue_event_crawler |  | s3_get_requests | 1 | 1/1 |
| 4 | glue_event_crawler |  | s3_get_requests | 100 | 1/1 |
| 4 | glue_event_crawler |  | s3_list_requests | 1 | 1/1 |
| 4 | glue_event_crawler |  | s3_list_requests | 100 | 1/1 |
| 5 | rtmsf | day_first | s3_head_requests | 100 | 1/1 |
| 5 | rtmsf | day_first | s3_get_requests | 100 | 1/1 |
| 5 | rtmsf | day_first | s3_list_requests | 100 | 1/1 |
| 5 | rtmsf | tenant_first | s3_head_requests | 100 | 1/1 |
| 5 | rtmsf | tenant_first | s3_get_requests | 100 | 1/1 |
| 5 | rtmsf | tenant_first | s3_list_requests | 100 | 1/1 |
| 5 | athena_msck | day_first | s3_head_requests | 100 | 1/1 |
| 5 | athena_msck | day_first | s3_get_requests | 100 | 1/1 |
| 5 | athena_msck | day_first | s3_list_requests | 100 | 1/1 |
| 5 | athena_msck | tenant_first | s3_head_requests | 100 | 1/1 |
| 5 | athena_msck | tenant_first | s3_get_requests | 100 | 1/1 |
| 5 | athena_msck | tenant_first | s3_list_requests | 100 | 1/1 |
| 2 | rtmsf |  | s3_read_list_requests | 100 | 1/1 |
| 2 | glue_event_crawler |  | s3_read_list_requests | 100 | 1/1 |
| 2 | athena_msck |  | s3_read_list_requests | 100 | 1/1 |
| 4 | rtmsf |  | s3_read_list_requests | 1 | 1/1 |
| 4 | rtmsf |  | s3_read_list_requests | 100 | 1/1 |
| 4 | glue_event_crawler |  | s3_read_list_requests | 1 | 1/1 |
| 4 | glue_event_crawler |  | s3_read_list_requests | 100 | 1/1 |
| 5 | rtmsf | day_first | s3_read_list_requests | 100 | 1/1 |
| 5 | rtmsf | tenant_first | s3_read_list_requests | 100 | 1/1 |
| 5 | athena_msck | day_first | s3_read_list_requests | 100 | 1/1 |
| 5 | athena_msck | tenant_first | s3_read_list_requests | 100 | 1/1 |

## Failed, incomplete, or unattempted trials

- duplicate_old_events, repetition 1: not attempted — TimeoutError: pilot budget expired before check started.
- rapid_changes, repetition 1: not attempted — TimeoutError: pilot budget expired before check started.

Settings, individual outcomes, and request-coverage details: [report_inputs.json](report_inputs.json).

Reproduce these reports without AWS calls from the repository root, substituting the path to the retained report directory:

```sh
uv run --extra experiment python -m experiments.figures --csv <report-directory>/graph_data.csv --results <report-directory>/report_inputs.json --output-dir regenerated
```
