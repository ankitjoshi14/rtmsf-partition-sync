-- Logical reconstruction reference derived from observations/catalog-schema.json.
-- Database ownership added after capture; original schema fingerprint is preserved
-- in export-provenance.json. Restore partition parameters/descriptors via Hive
-- Thrift as described in scripts/README.md before starting a worker.
-- This SQL alone is not a complete metadata restore and has not been run.
-- No data files are created by this script.
CREATE DATABASE `gcp_probe_r20261018_e504` LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-live-probe/r20261018-e504' WITH DBPROPERTIES ('gcp_probe_run_id'='r20261018-e504');
CREATE EXTERNAL TABLE `gcp_probe_r20261018_e504`.`catalog_probe` (`value` STRING)
PARTITIONED BY (`dt` STRING) STORED AS PARQUET
LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-live-probe/r20261018-e504'
TBLPROPERTIES ('EXTERNAL'='TRUE', 'external.table.purge'='false', 'gcp_probe_run_id'='r20261018-e504');

CREATE DATABASE `gcp_probe_r20261019_p1` LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-live-probe/r20261019-p1' WITH DBPROPERTIES ('gcp_probe_run_id'='r20261019-p1');
CREATE EXTERNAL TABLE `gcp_probe_r20261019_p1`.`catalog_probe` (`value` STRING)
PARTITIONED BY (`dt` STRING) STORED AS PARQUET
LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-live-probe/r20261019-p1'
TBLPROPERTIES ('EXTERNAL'='TRUE', 'external.table.purge'='false', 'gcp_probe_run_id'='r20261019-p1');

CREATE DATABASE `gcp_native_r20261019n1` LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data' WITH DBPROPERTIES ('gcp_probe_run_id'='r20261019n1');
CREATE EXTERNAL TABLE `gcp_native_r20261019n1`.`native_events` (`value` STRING)
PARTITIONED BY (`dt` STRING) STORED AS PARQUET
LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data'
TBLPROPERTIES ('EXTERNAL'='TRUE', 'external.table.purge'='false', 'gcp_probe_run_id'='r20261019n1');
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='a') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=a';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='b') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=b';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='burst_one') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=burst_one';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='marker_keep') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=marker_keep';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='marker_only') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=marker_only';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='marker_slash') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=marker_slash';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='nested_marker') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=nested_marker';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='raw+%20 space#?') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=raw+%20 space#?';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='recovery_add_after') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=recovery_add_after';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='recovery_add_before') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=recovery_add_before';
ALTER TABLE `gcp_native_r20261019n1`.`native_events` ADD PARTITION (`dt`='versions') LOCATION 'gs://rtmsf-gcp-lab-20261018-e504-fixtures/gcp-native/native-r20261019n1/data/dt=versions';
