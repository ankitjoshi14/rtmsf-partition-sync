# Activity validation

Bounded AWS validation; these repetitions do not establish tail latency or maximum sustainable capacity.

| Condition | Method | Repeat | PMCP attempts | HEAD / GET / LIST | First partition visible (s) | Drain (s) | Outcome |
|---|---|---:|---:|---|---:|---:|---|
| quick | rtmsf | 1 | 1 | 1 / 0 / 0 | 13.353 | 30.938 | passed |
| spaced | rtmsf | 1 | 2 | 1 / 0 / 1 | 32.510 | 29.564 | passed |
| distributed | rtmsf | 1 | 20 | 20 / 0 / 0 | 12.323 | 26.111 | passed |
| mixed | rtmsf | 1 | 11 | 1 / 0 / 10 | 14.453 | 37.675 | passed |
| continuous_1ps_10p | rtmsf | 1 | 120 | 10 / 0 / 110 | 12.064 | 32.761 | passed |
| continuous_1ps_20p | rtmsf | 1 | 120 | 20 / 0 / 100 | 12.574 | 28.008 | passed |
| continuous_3ps_10p | rtmsf | 1 | 130 | 10 / 0 / 120 | 33.110 | 25.485 | passed |
| continuous_3ps_20p | rtmsf | 1 | 170 | 20 / 0 / 150 | 33.187 | 25.215 | passed |
| load_1ps_1000p | rtmsf | 1 | 117 | 0 / 0 / 117 | unknown | 28.411 | passed |
| load_5ps_1000p | rtmsf | 1 | 582 | 0 / 0 / 582 | unknown | 32.635 | passed |
| load_20ps_10p | rtmsf | 1 | 240 | 0 / 0 / 240 | unknown | 177.392 | passed |
| load_20ps_1000p | rtmsf | 1 | 2332 | 0 / 0 / 2332 | unknown | 230.313 | passed |
| quick | immediate | 1 | 20 | 1 / 0 / 19 | 2.630 | 17.294 | passed |
| spaced | immediate | 1 | 20 | 1 / 0 / 19 | 2.432 | 14.771 | passed |
| distributed | immediate | 1 | 20 | 20 / 0 / 0 | 2.414 | 23.204 | passed |
| mixed | immediate | 1 | 20 | 1 / 0 / 19 | 1.131 | 21.983 | passed |
| quick | immediate | 2 | 20 | 1 / 0 / 19 | 2.337 | 16.983 | passed |
| spaced | immediate | 2 | 20 | 1 / 0 / 19 | 1.063 | 14.618 | passed |
| distributed | immediate | 2 | 20 | 20 / 0 / 0 | 2.558 | 14.922 | passed |
| mixed | immediate | 2 | 20 | 1 / 0 / 19 | 2.350 | 14.782 | passed |
| quick | rtmsf | 2 | 1 | 1 / 0 / 0 | 14.236 | 25.755 | passed |
| spaced | rtmsf | 2 | 2 | 1 / 0 / 1 | 33.409 | 34.613 | passed |
| distributed | rtmsf | 2 | 20 | 20 / 0 / 0 | 12.659 | 32.498 | passed |
| mixed | rtmsf | 2 | 11 | 1 / 0 / 10 | 13.919 | 37.646 | passed |
| continuous_1ps_10p | rtmsf | 2 | 120 | 10 / 0 / 110 | 11.534 | 34.063 | passed |
| continuous_1ps_20p | rtmsf | 2 | 120 | 20 / 0 / 100 | 12.310 | 32.806 | passed |
| continuous_3ps_10p | rtmsf | 2 | 130 | 10 / 0 / 120 | 33.396 | 40.654 | passed |
| continuous_3ps_20p | rtmsf | 2 | 170 | 20 / 0 / 150 | 32.867 | 34.971 | passed |
| load_1ps_1000p | rtmsf | 2 | 116 | 0 / 0 / 116 | unknown | 26.892 | passed |
| load_5ps_1000p | rtmsf | 2 | 581 | 0 / 0 / 581 | unknown | 35.843 | passed |
| load_20ps_10p | rtmsf | 2 | 243 | 0 / 0 / 243 | unknown | 184.547 | passed |
| load_20ps_1000p | rtmsf | 2 | 2326 | 0 / 0 / 2326 | unknown | 251.362 | passed |
| quick | rtmsf | 3 | 1 | 1 / 0 / 0 | 14.117 | 34.299 | passed |
| spaced | rtmsf | 3 | 2 | 1 / 0 / 1 | 33.641 | 33.601 | passed |
| distributed | rtmsf | 3 | 20 | 20 / 0 / 0 | 14.010 | 45.585 | passed |
| mixed | rtmsf | 3 | 11 | 1 / 0 / 10 | 13.877 | 35.385 | passed |
| continuous_1ps_10p | rtmsf | 3 | 120 | 10 / 0 / 110 | 12.715 | 29.800 | passed |
| continuous_1ps_20p | rtmsf | 3 | 120 | 20 / 0 / 100 | 12.599 | 40.578 | passed |
| continuous_3ps_10p | rtmsf | 3 | 130 | 10 / 0 / 120 | 33.090 | 38.933 | passed |
| continuous_3ps_20p | rtmsf | 3 | 170 | 20 / 0 / 150 | 32.615 | 39.686 | passed |
| load_1ps_1000p | rtmsf | 3 | 117 | 0 / 0 / 117 | unknown | 34.977 | passed |
| load_5ps_1000p | rtmsf | 3 | 582 | 0 / 0 / 582 | unknown | 48.461 | passed |
| load_20ps_10p | rtmsf | 3 | 240 | 0 / 0 / 240 | unknown | 172.158 | passed |
| load_20ps_1000p | rtmsf | 3 | 2336 | 0 / 0 / 2336 | unknown | 281.695 | passed |
| quick | immediate | 3 | 20 | 1 / 0 / 19 | 2.706 | 34.583 | passed |
| spaced | immediate | 3 | 20 | 1 / 0 / 19 | 1.228 | 26.297 | passed |
| distributed | immediate | 3 | 20 | 20 / 0 / 0 | 1.192 | 25.099 | passed |
| mixed | immediate | 3 | 20 | 1 / 0 / 19 | 2.511 | 27.695 | passed |

Visibility covers the first new partition, polled independently. Queue counts are approximate.
Drain includes observation/telemetry waits and eight seconds of observed queue emptiness.
S3 counts require agreement between CloudWatch, CloudTrail and worker API records.

Producer retries are separate from worker retries; write timing and delivered duplicate notifications include their effects.

Load tests retain the seed file in each partition; visibility is not measured for these already registered targets.
Backlog matches completed producer operations to observed ACKs, including operations whose notifications have not arrived.
ACK timing uses worker UTC timestamps and assumes synchronized clocks; these are observed bounds, not exact queue internals.

| Load | Repeat | Achieved ops/s | Maximum start delay (s) | Unacknowledged at write end | Oldest age (s) | Drain (s) |
|---|---:|---:|---:|---:|---:|---:|
| load_1ps_1000p | 1 | 1.007 | 0.150 | 11 | 9.956 | 28.411 |
| load_5ps_1000p | 1 | 5.000 | 0.151 | 57 | 11.180 | 32.635 |
| load_20ps_10p | 1 | 19.981 | 0.154 | 1400 | 92.103 | 177.392 |
| load_20ps_1000p | 1 | 19.991 | 0.153 | 1456 | 82.409 | 230.313 |
| load_1ps_1000p | 2 | 1.007 | 0.075 | 12 | 10.980 | 26.892 |
| load_5ps_1000p | 2 | 5.002 | 0.075 | 56 | 10.968 | 35.843 |
| load_20ps_10p | 2 | 19.987 | 0.145 | 1400 | 88.852 | 184.547 |
| load_20ps_1000p | 2 | 19.970 | 0.151 | 1612 | 92.279 | 251.362 |
| load_1ps_1000p | 3 | 1.006 | 0.150 | 11 | 9.859 | 34.977 |
| load_5ps_1000p | 3 | 4.999 | 0.150 | 56 | 11.391 | 48.461 |
| load_20ps_10p | 3 | 19.984 | 0.152 | 1400 | 82.800 | 172.158 |
| load_20ps_1000p | 3 | 19.979 | 0.150 | 1655 | 89.812 | 281.695 |
