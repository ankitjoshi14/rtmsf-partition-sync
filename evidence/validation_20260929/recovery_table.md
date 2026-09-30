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
