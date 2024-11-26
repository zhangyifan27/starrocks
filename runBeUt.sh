#!/usr/bin/env bash

echo "# Start run BE UT"
temp_file_be="beUtOutPut-"$(date "+%Y%m%d%H%M%S")".txt"
filter_be_ut="LakePrimaryKeyCompactionTest/LakePrimaryKeyCompactionTest.test_size_tiered_compaction_strategy*|SizeTieredCompactionPolicyTest.test_missed_two_version|SizeTieredCompactionPolicyTest.test_manual_force_base_compaction_less_min|TabletUpdatesTest*|TestParseMemSpec.Bad|UpdateManagerTest.test_on_rowset_finished|LakePrimaryKeyCompactionTest*|StreamOperatorsTest*|FastSchemaEvolutionTest.update_schema_id_test|RowsMapperTest.test_crm_file_gc|DiskSpaceMonitorTest*|LakePrimaryKeyPublishTest*|TestRuntimeProfile*|StarRocksMetricsTest*"
echo "filter_be_ut: ${filter_be_ut}"

filter_be_ut_in_docker="${filter_be_ut}"
nohup sh run-be-ut.sh --excluding-test-suit $filter_be_ut_in_docker > $temp_file_be 2>&1 &

