#!/usr/bin/env python
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

STAGE_CORE="core"
STAGE_LIBRARIES="libraries"
STAGE_BLINK_PLANNER="blink_planner"
STAGE_CONNECTORS="connectors"
STAGE_KAFKA_GELLY="kafka/gelly"
STAGE_TESTS="tests"
STAGE_MISC="misc"

MODULES_CORE="\
flink-annotations,\
flink-test-utils-parent/flink-test-utils,\
flink-state-backends/flink-statebackend-rocksdb,\
flink-clients,\
flink-core,\
flink-java,\
flink-optimizer,\
flink-runtime,\
flink-runtime-web,\
flink-scala,\
flink-streaming-java,\
flink-streaming-scala,\
flink-metrics,\
flink-metrics/flink-metrics-core"\
    .split(",")

MODULES_LIBRARIES="\
flink-libraries/flink-cep,\
flink-libraries/flink-cep-scala,\
flink-table/flink-table-common,\
flink-table/flink-table-api-java,\
flink-table/flink-table-api-scala,\
flink-table/flink-table-api-java-bridge,\
flink-table/flink-table-api-scala-bridge,\
flink-table/flink-table-planner,\
flink-table/flink-sql-client"\
    .split(",")

MODULES_BLINK_PLANNER="\
flink-table/flink-table-planner-blink,\
flink-table/flink-table-runtime-blink"\
    .split(",")

MODULES_CONNECTORS="\
flink-contrib/flink-connector-wikiedits,\
flink-filesystems,\
flink-filesystems/flink-fs-hadoop-shaded,\
flink-filesystems/flink-hadoop-fs,\
flink-filesystems/flink-mapr-fs,\
flink-filesystems/flink-oss-fs-hadoop,\
flink-filesystems/flink-s3-fs-base,\
flink-filesystems/flink-s3-fs-hadoop,\
flink-filesystems/flink-s3-fs-presto,\
flink-filesystems/flink-swift-fs-hadoop,\
flink-fs-tests,\
flink-formats,\
flink-formats/flink-avro-confluent-registry,\
flink-formats/flink-avro,\
flink-formats/flink-parquet,\
flink-formats/flink-sequence-file,\
flink-formats/flink-json,\
flink-formats/flink-csv,\
flink-connectors/flink-hbase,\
flink-connectors/flink-hcatalog,\
flink-connectors/flink-hadoop-compatibility,\
flink-connectors/flink-jdbc,\
flink-connectors,\
flink-connectors/flink-orc,\
flink-connectors/flink-connector-cassandra,\
flink-connectors/flink-connector-elasticsearch2,\
flink-connectors/flink-connector-elasticsearch5,\
flink-connectors/flink-connector-elasticsearch6,\
flink-connectors/flink-sql-connector-elasticsearch6,\
flink-connectors/flink-connector-elasticsearch-base,\
flink-connectors/flink-connector-filesystem,\
flink-connectors/flink-connector-kafka-0.9,\
flink-connectors/flink-sql-connector-kafka-0.9,\
flink-connectors/flink-connector-kafka-0.10,\
flink-connectors/flink-sql-connector-kafka-0.10,\
flink-connectors/flink-connector-kafka-0.11,\
flink-connectors/flink-sql-connector-kafka-0.11,\
flink-connectors/flink-connector-kafka-base,\
flink-connectors/flink-connector-nifi,\
flink-connectors/flink-connector-rabbitmq,\
flink-connectors/flink-connector-twitter,\
flink-metrics/flink-metrics-dropwizard,\
flink-metrics/flink-metrics-graphite,\
flink-metrics/flink-metrics-jmx,\
flink-metrics/flink-metrics-influxdb,\
flink-metrics/flink-metrics-prometheus,\
flink-metrics/flink-metrics-statsd,\
flink-metrics/flink-metrics-datadog,\
flink-metrics/flink-metrics-slf4j,\
flink-queryable-state/flink-queryable-state-runtime,\
flink-queryable-state/flink-queryable-state-client-java"\
    .split(",")

MODULES_KAFKA_GELLY="\
flink-libraries/flink-gelly,\
flink-libraries/flink-gelly-scala,\
flink-libraries/flink-gelly-examples,\
flink-connectors/flink-connector-kafka,\
flink-connectors/flink-sql-connector-kafka"\
    .split(",")

MODULES_JDK9_EXCLUSIONS="\
flink-filesystems/flink-s3-fs-hadoop,\
flink-filesystems/flink-s3-fs-presto,\
flink-filesystems/flink-mapr-fs,\
flink-connectors/flink-hbase"\
    .split(",")

MODULES_TESTS="\
flink-tests"\
    .split(",")

STAGE_MODULES = {
    STAGE_CORE: MODULES_CORE,
    STAGE_LIBRARIES: MODULES_LIBRARIES,
    STAGE_BLINK_PLANNER: MODULES_BLINK_PLANNER,
    STAGE_CONNECTORS: MODULES_CONNECTORS,
    STAGE_KAFKA_GELLY: MODULES_KAFKA_GELLY,
    STAGE_TESTS: MODULES_TESTS,
}

def get_mvn_modules(stage, profile):
    modules = STAGE_MODULES[stage][::]
    if stage == STAGE_CONNECTORS:
        if "include-kinesis" in profile:
            modules += ["flink-connectors/flink-connector-kinesis"]
        if "scala-2.11" in profile:
            # we can only build the Kafka 0.8 connector when building for Scala 2.11
            modules += ["flink-connectors/flink-connector-kafka-0.8"]
    elif stage == STAGE_CORE:
        if "scala-2.11" in profile:
            # we can only build the Scala Shell when building for Scala 2.11
            modules += ["flink-scala-shell"]
    return modules

def get_mvn_modules_for_test(stage, profile):
    modules = get_mvn_modules(stage, profile)
    if "jdk9" in profile:
        # exclude modules that fail tests on JDK 9
        modules = [module for module in modules if module not in MODULES_JDK9_EXCLUSIONS]
    return modules
