#!/usr/bin/env bash
# Run only after the lab's live catalog safety gate passes. The caller owns evidence;
# sudo authorizes only the query scope, never the ordinary worker.
set -eu
fail() { printf '%s\n' "$*" >&2; exit 2; }
[[ $# == 5 ]] || fail "usage: $0 thrift://PRIVATE_IPV4:9083 gs://RETURNED_WAREHOUSE DATABASE TABLE FRESH_EVIDENCE_DIRECTORY"
uri=$1 warehouse=$2 database=$3 table=$4 output=$5

[[ $uri =~ ^thrift://([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3}):9083$ ]] || fail 'endpoint must be a private IPv4 thrift URI on port 9083'
octets=("${BASH_REMATCH[@]:1}")
for octet in "${octets[@]}"; do
  (( 10#$octet <= 255 )) || fail 'endpoint must be a private IPv4 thrift URI on port 9083'
done
first=$((10#${octets[0]})) second=$((10#${octets[1]}))
(( first == 10 || (first == 172 && second >= 16 && second <= 31) || (first == 192 && second == 168) )) || fail 'endpoint must be a private IPv4 thrift URI on port 9083'
[[ $warehouse =~ ^gs://[a-z0-9][a-z0-9._-]+[a-z0-9](/[A-Za-z0-9._/-]*)?$ ]] || fail 'warehouse URI must be the returned gs:// bucket/path without SQL or shell syntax'
for identifier in "$database" "$table"; do
  [[ $identifier =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail 'database/table identifiers must contain only ASCII letters, digits and underscores, starting with a letter or underscore'
done
[[ -n $output && ! -e $output && ! -L $output ]] || fail 'use a fresh evidence directory; existing paths are never reused'

spark=/opt/rtmsf-query/spark-3.5.8-bin-hadoop3/bin/spark-sql
java=/usr/lib/jvm/java-11-openjdk-amd64
connector=/opt/rtmsf-query/gcs-connector-3.1.13-shaded.jar
[[ -x $spark && -x $java/bin/java && -r $connector && -x /usr/bin/time && -x /usr/bin/timeout ]] || fail 'required Spark, Java 11, connector, GNU time or timeout is missing'
command -v sudo >/dev/null && command -v systemd-run >/dev/null && command -v systemctl >/dev/null || fail 'sudo and systemd tools are required'
umask 077
mkdir -m 700 -- "$output" || fail 'use a fresh evidence directory; cannot reserve this path'
output=$(cd -- "$output" && pwd -P)
unit="rtmsf-query-$$-$(date +%s).scope"
printf 'SHOW PARTITIONS `%s`.`%s`;\nSELECT value,dt FROM `%s`.`%s` ORDER BY dt,value;\n' "$database" "$table" "$database" "$table" >"$output/query.sql"
cat >"$output/metadata.txt" <<EOF
started_utc=$(date -u +%FT%TZ)
endpoint=$uri
warehouse=$warehouse
database=$database
table=$table
spark=3.5.8
java_home=$java
hive_server=3.1.2
hive_client=3.1.3 (isolated Maven resolution)
gcs_connector=3.1.13 shaded
scope=$unit
memory_max=1200M
memory_swap_max=0
timeout_seconds=300
kill_grace_seconds=15
rss_source=time.txt GNU time maximum resident set size; not post-exit systemd memory
systemd.txt is a post-exit snapshot; a successful scope may already be unloaded
EOF
# Pre-create the file as the caller so the root query scope cannot change its owner.
: >"$output/time.txt"
status=0
sudo -n systemd-run --scope --unit="$unit" --property=MemoryMax=1200M --property=MemorySwapMax=0 \
  /usr/bin/time -v -o "$output/time.txt" \
  /usr/bin/timeout --signal=TERM --kill-after=15s 300s \
  /usr/bin/env JAVA_HOME="$java" \
  "$spark" --master 'local[1]' --driver-memory 512m --jars "$connector" \
  --conf spark.ui.enabled=false \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.hive.metastore.version=3.1.3 \
  --conf spark.sql.hive.metastore.jars=maven \
  --conf spark.sql.maven.additionalRemoteRepositories=https://repo.maven.apache.org/maven2/ \
  --conf "spark.hadoop.hive.metastore.uris=$uri" \
  --conf "spark.sql.warehouse.dir=$warehouse" \
  --conf "spark.hadoop.hive.metastore.warehouse.dir=$warehouse" \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.fs.gs.auth.type=COMPUTE_ENGINE \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.sql.hive.filesourcePartitionFileCacheSize=0 \
  -i /dev/null -f "$output/query.sql" \
  >"$output/stdout.txt" 2>"$output/stderr.txt" || status=$?
printf '%s\n' "$status" >"$output/exit-status.txt"
printf 'finished_utc=%s\n' "$(date -u +%FT%TZ)" >>"$output/metadata.txt"
/usr/bin/timeout --kill-after=2s 10s systemctl show "$unit" \
  --property=LoadState --property=ActiveState --property=SubState --property=Result \
  --property=MemoryMax --property=MemorySwapMax >"$output/systemd.txt" 2>&1 || true
printf 'evidence=%s exit_status=%s\n' "$output" "$status"
exit "$status"
