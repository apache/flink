#!/usr/bin/env bash
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

STAGE_COMPILE="compile"
STAGE_CORE="core"
STAGE_PYTHON="python"
STAGE_TABLE="table"
STAGE_CONNECTORS="connect"
STAGE_TESTS="tests"
STAGE_MISC="misc"
STAGE_CLEANUP="cleanup"

MODULES_CORE="\
flink-annotations,\
flink-test-utils-parent/flink-test-utils,\
flink-state-backends,\
flink-state-backends/flink-statebackend-changelog,\
flink-state-backends/flink-statebackend-heap-spillable,\
flink-state-backends/flink-statebackend-rocksdb,\
flink-state-backends/flink-statebackend-forst,\
flink-clients,\
flink-core,\
flink-rpc,\
flink-rpc/flink-rpc-core,\
flink-rpc/flink-rpc-akka,\
flink-rpc/flink-rpc-akka-loader,\
flink-runtime,\
flink-runtime-web,\
flink-streaming-java,\
flink-metrics,\
flink-metrics/flink-metrics-core,\
flink-external-resources,\
flink-external-resources/flink-external-resource-gpu,\
flink-libraries,\
flink-libraries/flink-cep,\
flink-libraries/flink-state-processing-api,\
flink-queryable-state,\
flink-queryable-state/flink-queryable-state-runtime,\
flink-queryable-state/flink-queryable-state-client-java,\
flink-container,\
flink-dstl,\
flink-dstl/flink-dstl-dfs,\
"

MODULES_TABLE="\
flink-table,\
flink-table/flink-sql-parser,\
flink-table/flink-table-common,\
flink-table/flink-table-api-java,\
flink-table/flink-table-api-scala,\
flink-table/flink-table-api-bridge-base,\
flink-table/flink-table-api-java-bridge,\
flink-table/flink-table-api-scala-bridge,\
flink-table/flink-table-api-java-uber,\
flink-table/flink-sql-client,\
flink-table/flink-sql-gateway-api,\
flink-table/flink-sql-gateway,\
flink-table/flink-table-planner,\
flink-table/flink-table-planner-loader,\
flink-table/flink-table-planner-loader-bundle,\
flink-table/flink-table-runtime,\
flink-table/flink-table-code-splitter,\
flink-table/flink-table-test-utils,\
"

MODULES_CONNECTORS="\
flink-filesystems,\
flink-filesystems/flink-azure-fs-hadoop,\
flink-filesystems/flink-fs-hadoop-shaded,\
flink-filesystems/flink-gs-fs-hadoop,\
flink-filesystems/flink-hadoop-fs,\
flink-filesystems/flink-oss-fs-hadoop,\
flink-filesystems/flink-s3-fs-base,\
flink-filesystems/flink-s3-fs-hadoop,\
flink-filesystems/flink-s3-fs-presto,\
flink-fs-tests,\
flink-formats,\
flink-formats/flink-format-common,\
flink-formats/flink-avro-confluent-registry,\
flink-formats/flink-sql-avro-confluent-registry,\
flink-formats/flink-avro,\
flink-formats/flink-sql-avro,\
flink-formats/flink-compress,\
flink-formats/flink-hadoop-bulk,\
flink-formats/flink-parquet,\
flink-formats/flink-sql-parquet,\
flink-formats/flink-sequence-file,\
flink-formats/flink-json,\
flink-formats/flink-csv,\
flink-formats/flink-orc,\
flink-formats/flink-sql-orc,\
flink-formats/flink-orc-nohive,\
flink-connectors/flink-file-sink-common,\
flink-connectors/flink-hadoop-compatibility,\
flink-connectors,\
flink-connectors/flink-connector-files,\
flink-metrics/flink-metrics-dropwizard,\
flink-metrics/flink-metrics-graphite,\
flink-metrics/flink-metrics-jmx,\
flink-metrics/flink-metrics-influxdb,\
flink-metrics/flink-metrics-prometheus,\
flink-metrics/flink-metrics-statsd,\
flink-metrics/flink-metrics-datadog,\
flink-metrics/flink-metrics-slf4j,\
flink-metrics/flink-metrics-otel,\
flink-connectors/flink-connector-base,\
"

MODULES_TESTS="\
flink-tests,\
"

function get_compile_modules_for_stage() {
    local stage=$1

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $MODULES_CORE -am"
        ;;
        (${STAGE_TABLE})
            echo "-pl $MODULES_TABLE -am"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $MODULES_CONNECTORS -am"
        ;;
        (${STAGE_TESTS})
            echo "-pl $MODULES_TESTS -am"
        ;;
        (${STAGE_MISC})
            # compile everything; using the -am switch does not work with negated module lists!
            # the negation takes precedence, thus not all required modules would be built
            echo ""
        ;;
        (${STAGE_PYTHON})
            # compile everything for PyFlink.
            echo ""
        ;;
    esac
}

function get_test_modules_for_stage() {
    local stage=$1

    local modules_core=$MODULES_CORE
    local modules_table=$MODULES_TABLE
    local modules_connectors=$MODULES_CONNECTORS
    local modules_tests=$MODULES_TESTS
    local negated_core=\!${MODULES_CORE//,/,\!}
    local negated_table=\!${MODULES_TABLE//,/,\!}
    local negated_connectors=\!${MODULES_CONNECTORS//,/,\!}
    local negated_tests=\!${MODULES_TESTS//,/,\!}
    local modules_misc="$negated_core,$negated_table,$negated_connectors,$negated_tests"

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $modules_core"
        ;;
        (${STAGE_TABLE})
            echo "-pl $modules_table"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $modules_connectors"
        ;;
        (${STAGE_TESTS})
            echo "-pl $modules_tests"
        ;;
        (${STAGE_MISC})
            echo "-pl $modules_misc"
        ;;
    esac
}
