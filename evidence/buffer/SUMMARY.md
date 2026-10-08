# Activity validation

Bounded AWS validation; these repetitions do not establish tail latency or maximum sustainable capacity.

| Condition | Method | Repeat | PMCP attempts | HEAD / GET / LIST | First partition visible (s) | Drain (s) | Outcome |
|---|---|---:|---:|---|---:|---:|---|
| load_20ps_10p (buffer 100) | rtmsf | 1 | 245 | 0 / 0 / 245 | unknown | 191.385 | passed |
| load_20ps_10p (buffer 1000) | rtmsf | 1 | 40 | 0 / 0 / 40 | unknown | 34.290 | passed |
| load_20ps_1000p (buffer 100) | rtmsf | 1 | 2332 | 0 / 0 / 2332 | unknown | 270.052 | passed |
| load_20ps_1000p (buffer 1000) | rtmsf | 1 | 2112 | 0 / 0 / 2111 | unknown | 77.285 | passed |
| load_20ps_10p (buffer 1000) | rtmsf | 2 | 40 | 0 / 0 / 40 | unknown | 33.064 | passed |
| load_20ps_10p (buffer 100) | rtmsf | 2 | 241 | 0 / 0 / 241 | unknown | 192.021 | passed |
| load_20ps_1000p (buffer 1000) | rtmsf | 2 | 2028 | 0 / 0 / 2028 | unknown | 85.727 | passed |
| load_20ps_1000p (buffer 100) | rtmsf | 2 | 2330 | 0 / 0 / 2330 | unknown | 273.600 | passed |

Visibility covers the first new partition, polled independently. Queue counts are approximate.
Drain includes observation/telemetry waits and eight seconds of observed queue emptiness.
S3 counts require agreement between CloudWatch, CloudTrail and worker API records.

Producer retries are separate from worker retries; write timing and delivered duplicate notifications include their effects.

Load tests retain the seed file in each partition; visibility is not measured for these already registered targets.
Backlog matches completed producer operations to observed ACKs, including operations whose notifications have not arrived.
ACK timing uses worker UTC timestamps and assumes synchronized clocks; these are observed bounds, not exact queue internals.

| Load | Repeat | Achieved ops/s | Maximum start delay (s) | Unacknowledged at write end | Oldest age (s) | Drain (s) |
|---|---:|---:|---:|---:|---:|---:|
| load_20ps_10p (buffer 100) | 1 | 19.974 | 0.151 | 1452 | 90.203 | 191.385 |
| load_20ps_10p (buffer 1000) | 1 | 19.972 | 0.151 | 325 | 16.769 | 34.290 |
| load_20ps_1000p (buffer 100) | 1 | 19.974 | 0.153 | 1624 | 95.009 | 270.052 |
| load_20ps_1000p (buffer 1000) | 1 | 19.983 | 0.150 | 1367 | 78.596 | 77.285 |
| load_20ps_10p (buffer 1000) | 2 | 19.963 | 0.172 | 383 | 19.212 | 33.064 |
| load_20ps_10p (buffer 100) | 2 | 19.981 | 0.154 | 1482 | 89.305 | 192.021 |
| load_20ps_1000p (buffer 1000) | 2 | 19.989 | 0.150 | 1274 | 75.447 | 85.727 |
| load_20ps_1000p (buffer 100) | 2 | 19.981 | 0.150 | 1624 | 91.136 | 273.600 |

Buffer comparison: fresh process per condition. RSS is the observed process high-water mark;
pending counts/bytes are sampled. Raw payload bytes exclude parsed objects and SDK/table memory.

| Condition | Buffer messages | Repeat | Peak RSS (MiB) | Sampled pending messages | Sampled payload (MiB) | Unacknowledged at write end | Drain (s) | Outcome |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| load_20ps_10p | 100 | 1 | 61.891 | 100 | 0.094 | 1452 | 191.385 | passed |
| load_20ps_10p | 1000 | 1 | 62.961 | 716 | 0.672 | 325 | 34.290 | passed |
| load_20ps_1000p | 100 | 1 | 62.145 | 100 | 0.094 | 1624 | 270.052 | passed |
| load_20ps_1000p | 1000 | 1 | 63.949 | 699 | 0.660 | 1367 | 77.285 | passed |
| load_20ps_10p | 1000 | 2 | 62.918 | 662 | 0.621 | 383 | 33.064 | passed |
| load_20ps_10p | 100 | 2 | 60.609 | 95 | 0.089 | 1482 | 192.021 | passed |
| load_20ps_1000p | 1000 | 2 | 64.207 | 835 | 0.788 | 1274 | 85.727 | passed |
| load_20ps_1000p | 100 | 2 | 62.145 | 100 | 0.094 | 1624 | 273.600 | passed |
