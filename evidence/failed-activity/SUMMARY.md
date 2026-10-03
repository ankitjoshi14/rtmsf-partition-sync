# Activity validation

Small AWS validation; one repetition does not establish stable latency or capacity limits.

| Condition | Method | Repeat | PMCP attempts | HEAD / GET / LIST | First partition visible (s) | Drain (s) | Outcome |
|---|---|---:|---:|---|---:|---:|---|
| quick | rtmsf | 1 | 1 | 1 / 0 / 0 | 15.344 | 32.387 | passed |
| spaced | rtmsf | 1 | 2 | 1 / 0 / 1 | 33.075 | 34.613 | passed |
| distributed | rtmsf | 1 | 20 | 20 / 0 / 0 | 13.724 | 30.687 | passed |
| mixed | rtmsf | 1 | 11 | 1 / 0 / 10 | 13.700 | 32.849 | passed |
| continuous_1ps_10p | rtmsf | 1 | 120 | 10 / 0 / 110 | 11.170 | 27.856 | passed |
| continuous_1ps_20p | rtmsf | 1 | 120 | 20 / 0 / 100 | 11.530 | 35.755 | passed |
| continuous_3ps_10p | rtmsf | 1 | 130 | 10 / 0 / 120 | 32.622 | 32.400 | passed |
| continuous_3ps_20p | rtmsf | 1 | 170 | 20 / 0 / 150 | 32.866 | 27.837 | passed |
| load_1ps_1000p | rtmsf | 1 | 117 | 0 / 0 / 117 | unknown | 30.607 | passed |
| load_5ps_1000p | rtmsf | 1 | 582 | 0 / 0 / 582 | unknown | 34.232 | passed |
| load_20ps_10p | rtmsf | 1 | unknown | 0 / 0 / 101 | unknown | unknown | failed |
| load_20ps_1000p | rtmsf | 1 | unknown | unknown / unknown / unknown | unknown | unknown | not_attempted |
| quick | immediate | 1 | unknown | unknown / unknown / unknown | unknown | unknown | not_attempted |
| spaced | immediate | 1 | unknown | unknown / unknown / unknown | unknown | unknown | not_attempted |
| distributed | immediate | 1 | unknown | unknown / unknown / unknown | unknown | unknown | not_attempted |
| mixed | immediate | 1 | unknown | unknown / unknown / unknown | unknown | unknown | not_attempted |

Visibility covers the first new partition, polled independently. Queue counts are approximate.
Drain includes observation/telemetry waits and eight seconds of observed queue emptiness.
S3 counts require agreement between CloudWatch, CloudTrail and worker API records.

Producer retries are separate from worker retries; write timing and delivered duplicate notifications include their effects.

Load tests retain the seed file in each partition; visibility is not measured for these already registered targets.
Backlog matches completed producer operations to observed ACKs, including operations whose notifications have not arrived.
ACK timing uses worker UTC timestamps and assumes synchronized clocks; these are observed bounds, not exact queue internals.

| Load | Repeat | Achieved ops/s | Maximum start delay (s) | Unacknowledged at write end | Oldest age (s) | Drain (s) |
|---|---:|---:|---:|---:|---:|---:|
| load_1ps_1000p | 1 | 1.007 | 0.150 | 12 | 10.935 | 30.607 |
| load_5ps_1000p | 1 | 4.999 | 0.179 | 57 | 11.174 | 34.232 |
| load_20ps_10p | 1 | unknown | unknown | unknown | unknown | unknown |
| load_20ps_1000p | 1 | unknown | unknown | unknown | unknown | unknown |
- load_20ps_10p/rtmsf: ConnectionClosedError: Connection was closed before we received a valid response from endpoint URL: "https://rtmsf-load-20ps-10p-rtmsf-p1000-r1-e104fcea34.s3.us-west-2.amazonaws.com/fixture/run_20260930_141608_ea3f/rtmsf_load_20ps_10p_rtmsf_r1_20260930_141608_ea3f/dt%3D2026-01-01/site%3Dsite-000/hour%3D01/part-1000161.parquet".
- ConnectionClosedError: Connection was closed before we received a valid response from endpoint URL: "https://rtmsf-load-20ps-10p-rtmsf-p1000-r1-e104fcea34.s3.us-west-2.amazonaws.com/fixture/run_20260930_141608_ea3f/rtmsf_load_20ps_10p_rtmsf_r1_20260930_141608_ea3f/dt%3D2026-01-01/site%3Dsite-000/hour%3D01/part-1000161.parquet".
