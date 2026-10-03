# Activity validation

Ankit Joshi

Small AWS validation; one repetition does not establish stable latency or capacity limits.

| Condition | Method | Repeat | PMCP attempts | HEAD / GET / LIST | First partition visible (s) | Drain (s) | Outcome |
|---|---|---:|---:|---|---:|---:|---|
| quick | rtmsf | 1 | 1 | 1 / 0 / 0 | 14.404 | 24.500 | passed |
| spaced | rtmsf | 1 | 2 | 1 / 0 / 1 | 32.773 | 28.281 | passed |
| distributed | rtmsf | 1 | 20 | 20 / 0 / 0 | 12.656 | 28.215 | passed |
| mixed | rtmsf | 1 | 11 | 1 / 0 / 10 | 15.039 | 28.290 | passed |
| continuous_1ps_10p | rtmsf | 1 | 120 | 10 / 0 / 110 | 11.068 | 26.039 | passed |
| continuous_1ps_20p | rtmsf | 1 | 120 | 20 / 0 / 100 | 12.430 | 33.166 | passed |
| continuous_3ps_10p | rtmsf | 1 | 130 | 10 / 0 / 120 | 32.973 | 40.815 | passed |
| continuous_3ps_20p | rtmsf | 1 | 170 | 20 / 0 / 150 | 33.384 | 26.692 | passed |
| load_1ps_1000p | rtmsf | 1 | 117 | 0 / 0 / 117 | unknown | 27.835 | passed |
| load_5ps_1000p | rtmsf | 1 | 581 | 0 / 0 / 581 | unknown | 27.663 | passed |
| load_20ps_10p | rtmsf | 1 | 240 | 0 / 0 / 240 | unknown | 180.060 | passed |
| load_20ps_1000p | rtmsf | 1 | 2317 | 0 / 0 / 2317 | unknown | 246.364 | passed |
| quick | immediate | 1 | 20 | 1 / 0 / 19 | 1.388 | 21.980 | passed |
| spaced | immediate | 1 | 20 | 1 / 0 / 19 | 1.120 | 16.364 | passed |
| distributed | immediate | 1 | 20 | 20 / 0 / 0 | 1.087 | 17.376 | passed |
| mixed | immediate | 1 | 20 | 1 / 0 / 19 | 2.437 | 23.104 | passed |

Visibility covers the first new partition, polled independently. Queue counts are approximate.
Drain includes observation/telemetry waits and eight seconds of observed queue emptiness.
S3 counts require agreement between CloudWatch, CloudTrail and worker API records.

Producer retries are separate from worker retries; write timing and delivered duplicate notifications include their effects.

Load tests retain the seed file in each partition; visibility is not measured for these already registered targets.
Backlog matches completed producer operations to observed ACKs, including operations whose notifications have not arrived.
ACK timing uses worker UTC timestamps and assumes synchronized clocks; these are observed bounds, not exact queue internals.

| Load | Repeat | Achieved ops/s | Maximum start delay (s) | Unacknowledged at write end | Oldest age (s) | Drain (s) |
|---|---:|---:|---:|---:|---:|---:|
| load_1ps_1000p | 1 | 1.007 | 0.150 | 12 | 10.941 | 27.835 |
| load_5ps_1000p | 1 | 4.999 | 0.150 | 57 | 11.254 | 27.663 |
| load_20ps_10p | 1 | 19.982 | 0.151 | 1400 | 83.653 | 180.060 |
| load_20ps_1000p | 1 | 19.986 | 0.152 | 1544 | 89.760 | 246.364 |
