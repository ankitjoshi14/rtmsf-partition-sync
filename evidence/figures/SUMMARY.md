# AWS pilot report

Run status: **passed**.


## Fixture layout

| Partition columns | Starting partitions | Occupied prefixes at each depth |
|---|---|---|
| day / tenant | 100 | 1 / 100 |
| day / tenant | 1000 | 1 / 1000 |
| day / tenant | 5000 | 1 / 5000 |
| day / tenant | 10000 | 1 / 10000 |
| dt / site / hour | 100 | 1 / 5 / 100 |
| dt / site / hour | 1000 | 1 / 42 / 1000 |
| dt / site / hour | 5000 | 3 / 209 / 5000 |
| dt / site / hour | 10000 | 5 / 417 / 10000 |
| tenant / day | 100 | 100 / 100 |
| tenant / day | 1000 | 1000 / 1000 |
| tenant / day | 5000 | 5000 / 5000 |
| tenant / day | 10000 | 10000 / 10000 |

Partition count and prefix structure vary together; compare layouts in separate runs.

## Graph outcomes

| Graph | Trial outcomes | Figures |
|---|---|---|
| 1 | 3/3 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_1.png) · [PDF](graph_1.pdf) |
| 2 | 60/60 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_2.png) · [PDF](graph_2.pdf) |
| 3 | 60/60 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_3.png) · [PDF](graph_3.pdf) |
| 4 | 30/30 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_4.png) · [PDF](graph_4.pdf) |
| 5 | 80/80 passed; 0 failed; 0 incomplete; 0 not attempted | [PNG](graph_5.png) · [PDF](graph_5.pdf) |

Graph 1 uses repetition 1, selected before running even if it fails. Binary traces are not averaged.
Graphs 2/4/5 show HEAD + GET + LIST totals on logarithmic axes, summed within each complete repetition before taking the median. Missing components make the total unknown.
Zero counts remain in the CSV and statistics; zero counts/ranges are labeled at the axis because logarithmic axes cannot display zero.
Graph 3 uses solid colored lines. Graphs 2–5 show one median point and thin min–max whiskers where repetitions differ. Individual values stay in the CSV; no p95 is estimated.
Graph 3 triggers: RTMSF successful upload response; crawler immediately before StartCrawler; MSCK immediately before StartQueryExecution. These are method-specific response times.
Blank values remain unknown. Observed CloudWatch HEAD/GET/LIST counts; delivery completeness is not guaranteed. Absent counters become zero only when recorded operations account for AllRequests; otherwise they remain blank. A trial pass does not establish exact accounting.

[Crash recovery](recovery_table.md) · [Supporting checks](checks_table.md)

## Measurement availability

Available counts exclude blanks. Plot samples use complete HEAD + GET + LIST totals, or visibility delays for Graph 3.
Counts cover recorded rows only; missing conditions do not establish a pass. Individual values remain in [graph_data.csv](graph_data.csv).

| Graph | Method | Layout | X conditions | Measurement rows available / recorded | Plot samples available / recorded | Usable repetitions per X |
|---|---|---|---|---|---|---|
| 2 | rtmsf |  | 4 | 60/60 | 20/20 | 5 |
| 2 | glue_event_crawler |  | 4 | 60/60 | 20/20 | 5 |
| 2 | athena_msck |  | 4 | 60/60 | 20/20 | 5 |
| 3 | rtmsf |  | 4 | 20/20 | 20/20 | 5 |
| 3 | glue_event_crawler |  | 4 | 20/20 | 20/20 | 5 |
| 3 | athena_msck |  | 4 | 20/20 | 20/20 | 5 |
| 4 | rtmsf |  | 3 | 45/45 | 15/15 | 5 |
| 4 | glue_event_crawler |  | 3 | 45/45 | 15/15 | 5 |
| 5 | rtmsf | day_first | 4 | 60/60 | 20/20 | 5 |
| 5 | rtmsf | tenant_first | 4 | 60/60 | 20/20 | 5 |
| 5 | athena_msck | day_first | 4 | 60/60 | 20/20 | 5 |
| 5 | athena_msck | tenant_first | 4 | 60/60 | 20/20 | 5 |

## Failed, incomplete, or unattempted trials

No failed or incomplete trial records were supplied. Missing records do not establish a pass.

Settings, individual outcomes, and request-coverage details: [report_inputs.json](report_inputs.json).

Reproduce these reports without AWS calls from the repository root, substituting the path to the retained report directory:

```sh
uv run --extra experiment python -m experiments.figures --csv <report-directory>/graph_data.csv --results <report-directory>/report_inputs.json --output-dir regenerated
```
