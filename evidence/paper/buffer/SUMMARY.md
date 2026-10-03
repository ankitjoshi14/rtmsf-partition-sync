# Activity validation

Ankit Joshi

Small AWS validation; one repetition does not establish stable latency or capacity limits.

| Condition | Method | Repeat | PMCP attempts | HEAD / GET / LIST | First partition visible (s) | Drain (s) | Outcome |
|---|---|---:|---:|---|---:|---:|---|
| load_20ps_10p (buffer 100) | rtmsf | 1 | 246 | 0 / 0 / 246 | unknown | 215.609 | passed |
| load_20ps_10p (buffer 1000) | rtmsf | 1 | 40 | 0 / 0 / 40 | unknown | 27.137 | passed |
| load_20ps_1000p (buffer 100) | rtmsf | 1 | 2327 | 0 / 0 / 2327 | unknown | 268.403 | passed |
| load_20ps_1000p (buffer 1000) | rtmsf | 1 | 2081 | 0 / 0 / 2081 | unknown | 77.622 | passed |
| load_20ps_10p (buffer 1000) | rtmsf | 2 | 40 | 0 / 0 / 40 | unknown | 32.049 | passed |
| load_20ps_10p (buffer 100) | rtmsf | 2 | 244 | 0 / 0 / 244 | unknown | 194.088 | passed |
| load_20ps_1000p (buffer 1000) | rtmsf | 2 | 2020 | 0 / 0 / 2018 | unknown | 88.927 | passed |
| load_20ps_1000p (buffer 100) | rtmsf | 2 | 2318 | 0 / 0 / 2318 | unknown | 274.466 | passed |

Visibility covers the first new partition, polled independently. Queue counts are approximate.
Drain includes observation/telemetry waits and eight seconds of observed queue emptiness.
S3 counts require agreement between CloudWatch, CloudTrail and worker API records.

Producer retries are separate from worker retries; write timing and delivered duplicate notifications include their effects.

Load tests retain the seed file in each partition; visibility is not measured for these already registered targets.
Backlog matches completed producer operations to observed ACKs, including operations whose notifications have not arrived.
ACK timing uses worker UTC timestamps and assumes synchronized clocks; these are observed bounds, not exact queue internals.

| Load | Repeat | Achieved ops/s | Maximum start delay (s) | Unacknowledged at write end | Oldest age (s) | Drain (s) |
|---|---:|---:|---:|---:|---:|---:|
| load_20ps_10p (buffer 100) | 1 | 19.960 | 0.186 | 1513 | 93.187 | 215.609 |
| load_20ps_10p (buffer 1000) | 1 | 19.981 | 0.105 | 192 | 9.886 | 27.137 |
| load_20ps_1000p (buffer 100) | 1 | 19.974 | 0.250 | 1619 | 88.065 | 268.403 |
| load_20ps_1000p (buffer 1000) | 1 | 19.981 | 0.127 | 1390 | 77.597 | 77.622 |
| load_20ps_10p (buffer 1000) | 2 | 19.987 | 0.157 | 354 | 17.857 | 32.049 |
| load_20ps_10p (buffer 100) | 2 | 19.939 | 0.236 | 1500 | 108.761 | 194.088 |
| load_20ps_1000p (buffer 1000) | 2 | 19.982 | 0.150 | 1345 | 73.297 | 88.927 |
| load_20ps_1000p (buffer 100) | 2 | 19.974 | 0.166 | 1642 | 93.291 | 274.466 |

Buffer comparison: fresh process per condition. RSS is the observed process high-water mark;
pending counts/bytes are sampled. Raw payload bytes exclude parsed objects and SDK/table memory.

| Condition | Buffer messages | Repeat | Peak RSS (MiB) | Sampled pending messages | Sampled payload (MiB) | Unacknowledged at write end | Drain (s) | Outcome |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| load_20ps_10p | 100 | 1 | 60.504 | 99 | 0.093 | 1513 | 215.609 | passed |
| load_20ps_10p | 1000 | 1 | 61.770 | 784 | 0.735 | 192 | 27.137 | passed |
| load_20ps_1000p | 100 | 1 | 62.145 | 98 | 0.092 | 1619 | 268.403 | passed |
| load_20ps_1000p | 1000 | 1 | 64.207 | 844 | 0.796 | 1390 | 77.622 | passed |
| load_20ps_10p | 1000 | 2 | 62.918 | 698 | 0.655 | 354 | 32.049 | passed |
| load_20ps_10p | 100 | 2 | 60.805 | 97 | 0.091 | 1500 | 194.088 | passed |
| load_20ps_1000p | 1000 | 2 | 62.828 | 849 | 0.801 | 1345 | 88.927 | passed |
| load_20ps_1000p | 100 | 2 | 62.125 | 96 | 0.090 | 1642 | 274.466 | passed |
