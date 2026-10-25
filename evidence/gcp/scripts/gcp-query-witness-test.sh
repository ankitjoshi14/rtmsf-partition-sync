#!/usr/bin/env bash
set -eu
script="$(cd -- "$(dirname -- "$0")" && pwd)/gcp-query-witness.sh"
tmp=$(mktemp -d)
tmp=$(cd -- "$tmp" && pwd -P)
trap 'rm -rf -- "$tmp"' EXIT

reject() {
  local message=$1
  shift
  local status=0
  bash "$script" "$@" >"$tmp/stdout" 2>"$tmp/stderr" || status=$?
  [[ $status == 2 ]] || { cat "$tmp/stderr"; echo "expected validation exit 2, got $status"; exit 1; }
  [[ $(cat "$tmp/stderr") == *"$message"* ]] || { cat "$tmp/stderr"; exit 1; }
}

for uri in 'thrift://127.0.0.1:9083' 'thrift://8.8.8.8:9083' 'thrift://172.15.1.1:9083' \
  'thrift://10.0.0.256:9083' 'thrift://metastore:9083' 'thrift://10.0.0.1:9084' \
  'thrift://10.0.0.1:9083; id'; do
  reject 'private IPv4' "$uri" gs://lab-bucket/hive-warehouse lab fixture "$tmp/fresh"
done
reject 'warehouse URI' thrift://10.0.0.1:9083 'gs://bucket/path; DROP TABLE x' lab fixture "$tmp/fresh"
reject 'database/table' thrift://10.0.0.1:9083 gs://lab-bucket/hive-warehouse 'lab;DROP' fixture "$tmp/fresh"
reject 'database/table' thrift://10.0.0.1:9083 gs://lab-bucket/hive-warehouse lab 'fixture`' "$tmp/fresh"
[[ ! -e $tmp/fresh ]]

mkdir "$tmp/used"
printf '%s\n' retained >"$tmp/used/marker"
for uri in thrift://10.1.2.3:9083 thrift://172.16.0.1:9083 thrift://172.31.255.254:9083 thrift://192.168.1.2:9083; do
  reject 'fresh evidence directory' "$uri" gs://lab-bucket/hive-warehouse lab fixture "$tmp/used"
done
[[ $(cat "$tmp/used/marker") == retained ]]
ln -s "$tmp/missing" "$tmp/symlink"
reject 'fresh evidence directory' thrift://10.1.2.3:9083 gs://lab-bucket/hive-warehouse lab fixture "$tmp/symlink"
[[ -L $tmp/symlink && ! -e $tmp/missing ]]
printf '%s\n' 'PASS: private endpoint/SQL/warehouse validation and evidence no-reuse guard (no Spark/cloud calls).'

# Replace fixed runtime paths only in a disposable copy; no production test options.
mkdir -p "$tmp/bin" "$tmp/java/bin"
: >"$tmp/connector.jar"
cat >"$tmp/bin/fake" <<'FAKE'
#!/usr/bin/env bash
set -eu
case ${0##*/} in
  sudo) [[ $1 == -n ]]; shift; exec "$@" ;;
  systemd-run)
    [[ $1 == --scope && $2 == --unit=rtmsf-query-*.scope && $3 == --property=MemoryMax=1200M && $4 == --property=MemorySwapMax=0 ]]
    shift 4; exec "$@" ;;
  time)
    [[ $1 == -v && $2 == -o ]]
    printf '%s\n' 'Maximum resident set size (kbytes): 123' >"$3"
    shift 3; exec "$@" ;;
  timeout)
    if [[ $1 == --signal=TERM ]]; then
      [[ $2 == --kill-after=15s && $3 == 300s ]]; shift 3
    else
      [[ $1 == --kill-after=2s && $2 == 10s ]]; shift 2
    fi
    exec "$@" ;;
  systemctl) printf 'Result=fake-%s\n' "$FAKE_QUERY_EXIT" ;;
  spark)
    [[ $JAVA_HOME == $FAKE_JAVA_HOME ]]
    printf '%s\0' "$@" >"$FAKE_ARGS"
    printf 'value\tdt\n'; printf 'fake diagnostic\n' >&2
    exit "$FAKE_QUERY_EXIT" ;;
  java) exit 0 ;;
  *) exit 99 ;;
esac
FAKE
chmod +x "$tmp/bin/fake"
for executable in sudo systemd-run systemctl time timeout spark; do
  ln -s fake "$tmp/bin/$executable"
done
ln -s "$tmp/bin/fake" "$tmp/java/bin/java"
sed -e "s|spark=/opt/rtmsf-query/spark-3.5.8-bin-hadoop3/bin/spark-sql|spark=$tmp/bin/spark|" \
  -e "s|java=/usr/lib/jvm/java-11-openjdk-amd64|java=$tmp/java|" \
  -e "s|connector=/opt/rtmsf-query/gcs-connector-3.1.13-shaded.jar|connector=$tmp/connector.jar|" \
  -e "s|/usr/bin/timeout|$tmp/bin/timeout|g" -e "s|/usr/bin/time|$tmp/bin/time|g" \
  "$script" >"$tmp/witness.sh"
export PATH="$tmp/bin:$PATH" FAKE_ARGS="$tmp/actual-args" FAKE_JAVA_HOME="$tmp/java"
expected_args=(--master 'local[1]' --driver-memory 512m --jars "$tmp/connector.jar"
  --conf spark.ui.enabled=false
  --conf spark.driver.host=127.0.0.1
  --conf spark.driver.bindAddress=127.0.0.1
  --conf spark.sql.catalogImplementation=hive
  --conf spark.sql.hive.metastore.version=3.1.3
  --conf spark.sql.hive.metastore.jars=maven
  --conf spark.sql.maven.additionalRemoteRepositories=https://repo.maven.apache.org/maven2/
  --conf spark.hadoop.hive.metastore.uris=thrift://10.0.0.1:9083
  --conf spark.sql.warehouse.dir=gs://lab-bucket/hive-warehouse
  --conf spark.hadoop.hive.metastore.warehouse.dir=gs://lab-bucket/hive-warehouse
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
  --conf spark.hadoop.fs.gs.auth.type=COMPUTE_ENGINE
  --conf spark.sql.shuffle.partitions=1
  --conf spark.sql.hive.filesourcePartitionFileCacheSize=0
  -i /dev/null)
for expected in 0 7; do
  output="$tmp/result-$expected"
  export FAKE_QUERY_EXIT=$expected
  status=0
  bash "$tmp/witness.sh" thrift://10.0.0.1:9083 gs://lab-bucket/hive-warehouse lab fixture "$output" \
    >"$tmp/runner-stdout" 2>"$tmp/runner-stderr" || status=$?
  [[ $status == "$expected" && $(cat "$output/exit-status.txt") == "$expected" ]]
  [[ $(cat "$output/stdout.txt") == $'value\tdt' && $(cat "$output/stderr.txt") == 'fake diagnostic' ]]
  [[ $(cat "$output/time.txt") == 'Maximum resident set size (kbytes): 123' ]]
  [[ $(cat "$output/systemd.txt") == "Result=fake-$expected" ]]
  [[ $(cat "$output/query.sql") == $'SHOW PARTITIONS `lab`.`fixture`;\nSELECT value,dt FROM `lab`.`fixture` ORDER BY dt,value;' ]]
  [[ $(cat "$output/metadata.txt") == *'hive_client=3.1.3 (isolated Maven resolution)'* ]]
  printf '%s\0' "${expected_args[@]}" -f "$output/query.sql" >"$tmp/expected-args"
  cmp "$tmp/expected-args" "$FAKE_ARGS"
  status=0
  bash "$tmp/witness.sh" thrift://10.0.0.1:9083 gs://lab-bucket/hive-warehouse lab fixture "$output" \
    >"$tmp/runner-stdout" 2>"$tmp/runner-stderr" || status=$?
  [[ $status == 2 && $(cat "$output/exit-status.txt") == "$expected" ]]
done
printf '%s\n' 'PASS: exact invocation, successful/nonzero query evidence, exit propagation and reuse protection with fake executables (no Spark/cloud calls).'
