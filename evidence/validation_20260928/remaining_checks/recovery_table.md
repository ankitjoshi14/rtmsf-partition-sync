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
