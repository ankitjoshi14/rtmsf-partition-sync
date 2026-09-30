# AWS pilot report

Author: Ankit Joshi

Run status: **passed**.


## Graph outcomes

| Graph | Trial outcomes | Figures |
|---|---|---|

Graph 1 uses repetition 1, selected before running even if it fails. Binary traces are not averaged.
Graphs 2/4/5 show HEAD + GET + LIST totals on logarithmic axes, summed within each complete repetition before taking the median. Missing components make the total unknown.
Zero counts remain in the CSV and statistics; zero counts/ranges are labeled at the axis because logarithmic axes cannot display zero.
Graph 3 uses solid colored lines. Graphs 2–5 show one median point and thin min–max whiskers where repetitions differ. Individual values stay in the CSV; no p95 is estimated.
Graph 3 triggers: RTMSF successful upload response; crawler immediately before StartCrawler; MSCK immediately before StartQueryExecution. These are method-specific response times.
Blank values remain unknown. Observed CloudWatch HEAD/GET/LIST counts; delivery completeness is not guaranteed. Absent counters become zero only when recorded operations account for AllRequests; otherwise they remain blank. A trial pass does not establish exact accounting.

## Table 1. Crash recovery

| Crash case | Expected after recovery | Observed counts | Crash verified / attempted | Replayed + acknowledged / attempted | Passed / attempted |
|---|---|---|---|---|---|
| ADD — before processing | Registered | Not attempted | 0/0 | 0/0 | 0/0 |
| ADD — after registration, before acknowledgement | Registered | Not attempted | 0/0 | 0/0 | 0/0 |
| DROP — before processing | Absent | Not attempted | 0/0 | 0/0 | 0/0 |
| DROP — after removal, before acknowledgement | Absent | Not attempted | 0/0 | 0/0 | 0/0 |

- ADD — before processing: 0/1 attempted; 0 failed; 0 incomplete.
- ADD — after registration, before acknowledgement: 0/1 attempted; 0 failed; 0 incomplete.
- DROP — before processing: 0/1 attempted; 0 failed; 0 incomplete.
- DROP — after removal, before acknowledgement: 0/1 attempted; 0 failed; 0 incomplete.

Observed states are measured after recovery, before the next cycle step or cleanup. Skipped trials are not attempts.

## Supporting checks

| Check | Passed / attempted | Failed | Incomplete | Not attempted |
|---|---|---|---|---|
| duplicate_old_events | 1/1 | 0 | 0 | 0 |
| rapid_changes | 1/1 | 0 | 0 | 0 |

## Measurement availability

Available / recorded counts exclude blanks from the available sample size. A 0/0 condition has no point records.

| Graph | Method | Layout | Measurement | X | Available / recorded |
|---|---|---|---|---|---|

## Failed, incomplete, or unattempted trials

No failed or incomplete trial records were supplied. Missing records do not establish a pass.

Settings, individual outcomes, and request-coverage details: [report_inputs.json](report_inputs.json).

Reproduce these reports without AWS calls from the repository root, substituting the path to the retained report directory:

```sh
uv run --extra experiment python -m experiments.figures --csv <report-directory>/graph_data.csv --results <report-directory>/report_inputs.json --output-dir regenerated
```
