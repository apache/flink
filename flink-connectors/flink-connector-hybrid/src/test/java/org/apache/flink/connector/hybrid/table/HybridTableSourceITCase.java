/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hybrid.table;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

/** Integration test for the {@link HybridTableSource}. */
@ExtendWith(TestLoggerExtension.class)
public class HybridTableSourceITCase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static final List<String> DATA_IDS = new ArrayList<>();

    static {
        List<Row> data0 = new ArrayList<>();
        data0.add(Row.of("hello_a", "flink_a", 0L));
        data0.add(Row.of("hello_a", "hadoop_a", 1L));
        data0.add(Row.of("hello_a", "world_a", 2L));

        List<Row> data1 = new ArrayList<>();
        data1.add(Row.of("hello_b", "flink_b", 3L));
        data1.add(Row.of("hello_b", "hadoop_b", 4L));
        data1.add(Row.of("hello_b", "world_b", 5L));

        List<Row> data2 = new ArrayList<>();
        data2.add(Row.of("hello_c", "flink_c", 6L));
        data2.add(Row.of("hello_c", "hadoop_c", 7L));
        data2.add(Row.of("hello_c", "world_c", 8L));

        List<Row> dataWithMetadata0 = new ArrayList<>();
        dataWithMetadata0.add(Row.of("hello_d", "flink_d", 9L, "meta0"));
        dataWithMetadata0.add(Row.of("hello_d", "hadoop_d", 10L, "meta1"));
        dataWithMetadata0.add(Row.of("hello_d", "world_d", 11L, "meta2"));

        List<Row> dataWithMetadata1 = new ArrayList<>();
        dataWithMetadata1.add(Row.of("hello_e", "flink_e", 12L, "meta3"));
        dataWithMetadata1.add(Row.of("hello_e", "hadoop_e", 13L, "meta4"));
        dataWithMetadata1.add(Row.of("hello_e", "world_e", 14L, "meta5"));

        List<Row> dataWithMetadata2 = new ArrayList<>();
        dataWithMetadata2.add(Row.of("hello_e", "flink_e", 15L, null));
        dataWithMetadata2.add(Row.of("hello_e", "hadoop_e", 16L, null));
        dataWithMetadata2.add(Row.of("hello_e", "world_e", 17L, null));

        String dataId0 = TestValuesTableFactory.registerData(data0);
        String dataId1 = TestValuesTableFactory.registerData(data1);
        String dataId2 = TestValuesTableFactory.registerData(data2);
        String dataId3 = TestValuesTableFactory.registerData(dataWithMetadata0);
        String dataId4 = TestValuesTableFactory.registerData(dataWithMetadata1);
        String dataId5 = TestValuesTableFactory.registerData(dataWithMetadata2);
        DATA_IDS.add(dataId0);
        DATA_IDS.add(dataId1);
        DATA_IDS.add(dataId2);
        DATA_IDS.add(dataId3);
        DATA_IDS.add(dataId4);
        DATA_IDS.add(dataId5);
    }

    private static Stream<EnvMode> envs() {
        return Stream.of(EnvMode.Streaming, EnvMode.Batch);
    }

    private static TableEnvironment getTableEnv(EnvMode envMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (envMode == EnvMode.Streaming) {
            return StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
        } else {
            return StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        }
    }

    // -------------------------------------------------------------------------------------
    // HybridTableSource tests
    // -------------------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("envs")
    void testHybridSourceWithDDL(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        tEnv.executeSql(
                "CREATE TABLE hybrid_source("
                        + "  f0 varchar,"
                        + "  f1 varchar,"
                        + "  f2 bigint"
                        + ") WITH ("
                        + "  'connector' = 'hybrid',"
                        + "  'source-identifiers' = 'historical,realtime',"
                        + "  'historical.connector' = 'values',"
                        + "  'historical.data-id' = '"
                        + DATA_IDS.get(0)
                        + "',"
                        + "  'historical.runtime-source' = 'NewSource',"
                        + "  'historical.bounded' = 'true',"
                        + "  'realtime.connector' = 'values',"
                        + "  'realtime.data-id' = '"
                        + DATA_IDS.get(1)
                        + "',"
                        + "  'realtime.runtime-source' = 'NewSource'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT * FROM hybrid_source");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_a, flink_a, 0]\n"
                        + "+I[hello_a, hadoop_a, 1]\n"
                        + "+I[hello_a, world_a, 2]\n"
                        + "+I[hello_b, flink_b, 3]\n"
                        + "+I[hello_b, hadoop_b, 4]\n"
                        + "+I[hello_b, world_b, 5]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void testHybridSourceWithTable(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        TableDescriptor tableDescriptor =
                TableDescriptor.forConnector("hybrid")
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.STRING())
                                        .column("f2", DataTypes.BIGINT())
                                        .build())
                        .option("source-identifiers", "historical,realtime")
                        .option("historical.connector", "values")
                        .option("historical.data-id", DATA_IDS.get(0))
                        .option("historical.runtime-source", "NewSource")
                        .option("historical.bounded", "true")
                        .option("realtime.connector", "values")
                        .option("realtime.data-id", DATA_IDS.get(1))
                        .option("realtime.runtime-source", "NewSource")
                        .build();
        tEnv.createTable("hybrid_source", tableDescriptor);

        Table table = tEnv.from("hybrid_source").select($("f0"), $("f1"), $("f2"));

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_a, flink_a, 0]\n"
                        + "+I[hello_a, hadoop_a, 1]\n"
                        + "+I[hello_a, world_a, 2]\n"
                        + "+I[hello_b, flink_b, 3]\n"
                        + "+I[hello_b, hadoop_b, 4]\n"
                        + "+I[hello_b, world_b, 5]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void testMultiSourcesWithDDL(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        tEnv.executeSql(
                "CREATE TABLE hybrid_source("
                        + "  f0 varchar,"
                        + "  f1 varchar,"
                        + "  f2 bigint"
                        + ") WITH ("
                        + "  'connector' = 'hybrid',"
                        + "  'source-identifiers' = 'historical01,historical02,realtime',"
                        + "  'historical01.connector' = 'values',"
                        + "  'historical01.data-id' = '"
                        + DATA_IDS.get(0)
                        + "',"
                        + "  'historical01.runtime-source' = 'NewSource',"
                        + "  'historical02.connector' = 'values',"
                        + "  'historical02.data-id' = '"
                        + DATA_IDS.get(1)
                        + "',"
                        + "  'historical02.runtime-source' = 'NewSource',"
                        + "  'realtime.connector' = 'values',"
                        + "  'realtime.data-id' = '"
                        + DATA_IDS.get(2)
                        + "',"
                        + "  'realtime.runtime-source' = 'NewSource'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT * FROM hybrid_source");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_a, flink_a, 0]\n"
                        + "+I[hello_a, hadoop_a, 1]\n"
                        + "+I[hello_a, world_a, 2]\n"
                        + "+I[hello_b, flink_b, 3]\n"
                        + "+I[hello_b, hadoop_b, 4]\n"
                        + "+I[hello_b, world_b, 5]\n"
                        + "+I[hello_c, flink_c, 6]\n"
                        + "+I[hello_c, hadoop_c, 7]\n"
                        + "+I[hello_c, world_c, 8]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void testMultiSourcesWithTableApi(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        TableDescriptor tableDescriptor =
                TableDescriptor.forConnector("hybrid")
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", DataTypes.STRING())
                                        .column("f1", DataTypes.STRING())
                                        .column("f2", DataTypes.BIGINT())
                                        .build())
                        .option("source-identifiers", "historical01,historical02,realtime")
                        .option("historical01.connector", "values")
                        .option("historical01.data-id", DATA_IDS.get(0))
                        .option("historical01.runtime-source", "NewSource")
                        .option("historical02.connector", "values")
                        .option("historical02.data-id", DATA_IDS.get(1))
                        .option("historical02.runtime-source", "NewSource")
                        .option("realtime.connector", "values")
                        .option("realtime.data-id", DATA_IDS.get(2))
                        .option("realtime.runtime-source", "NewSource")
                        .build();
        tEnv.createTable("hybrid_source", tableDescriptor);

        Table table = tEnv.from("hybrid_source").select($("f0"), $("f1"), $("f2"));

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_a, flink_a, 0]\n"
                        + "+I[hello_a, hadoop_a, 1]\n"
                        + "+I[hello_a, world_a, 2]\n"
                        + "+I[hello_b, flink_b, 3]\n"
                        + "+I[hello_b, hadoop_b, 4]\n"
                        + "+I[hello_b, world_b, 5]\n"
                        + "+I[hello_c, flink_c, 6]\n"
                        + "+I[hello_c, hadoop_c, 7]\n"
                        + "+I[hello_c, world_c, 8]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void testSwitchedStartPosition(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        // when switched-start-position-enabled is ture, the first source enumerator need implement
        // SupportsGetEndTimestamp, next source need implement SupportsSwitchedStartTimestamp
        tEnv.executeSql(
                "CREATE TABLE hybrid_source("
                        + "  f0 varchar,"
                        + "  f1 varchar,"
                        + "  f2 bigint"
                        + ") WITH ("
                        + "  'connector' = 'hybrid',"
                        + "  'source-identifiers' = 'historical,realtime',"
                        + "  'switched-start-position-enabled' = 'true',"
                        + "  'historical.connector' = 'values',"
                        + "  'historical.data-id' = '"
                        + DATA_IDS.get(0)
                        + "',"
                        + "  'historical.runtime-source' = 'NewSource',"
                        + "  'realtime.connector' = 'values',"
                        + "  'realtime.data-id' = '"
                        + DATA_IDS.get(1)
                        + "',"
                        + "  'realtime.runtime-source' = 'NewSource'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT * FROM hybrid_source");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_a, flink_a, 0]\n"
                        + "+I[hello_a, hadoop_a, 1]\n"
                        + "+I[hello_a, world_a, 2]\n"
                        + "+I[hello_b, flink_b, 3]\n"
                        + "+I[hello_b, hadoop_b, 4]\n"
                        + "+I[hello_b, world_b, 5]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void tesHybridSourceProjectPushDown(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        // 2 sources both support project pushdown (pushdown option is true by default)
        tEnv.executeSql(
                "CREATE TABLE hybrid_source("
                        + "  f0 varchar,"
                        + "  f1 varchar,"
                        + "  f2 bigint"
                        + ") WITH ("
                        + "  'connector' = 'hybrid',"
                        + "  'source-identifiers' = 'historical,realtime',"
                        + "  'historical.connector' = 'values',"
                        + "  'historical.data-id' = '"
                        + DATA_IDS.get(0)
                        + "',"
                        + "  'historical.runtime-source' = 'NewSource',"
                        + "  'historical.bounded' = 'true',"
                        + "  'realtime.connector' = 'values',"
                        + "  'realtime.data-id' = '"
                        + DATA_IDS.get(1)
                        + "',"
                        + "  'realtime.runtime-source' = 'NewSource'"
                        + ")");

        // just select 2 fields to test project pushdown
        Table table = tEnv.sqlQuery("SELECT f0, f1 FROM hybrid_source");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_a, flink_a]\n"
                        + "+I[hello_a, hadoop_a]\n"
                        + "+I[hello_a, world_a]\n"
                        + "+I[hello_b, flink_b]\n"
                        + "+I[hello_b, hadoop_b]\n"
                        + "+I[hello_b, world_b]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void tesHybridSourceAllSourcesHasDDLMetaData(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        // 2 sources has a common metadata
        tEnv.executeSql(
                "CREATE TABLE hybrid_source("
                        + "  f0 varchar,"
                        + "  f1 varchar,"
                        + "  f2 bigint,"
                        + "  metadata_1 varchar metadata"
                        + ") WITH ("
                        + "  'connector' = 'hybrid',"
                        + "  'source-identifiers' = 'historical,realtime',"
                        + "  'historical.connector' = 'values',"
                        + "  'historical.data-id' = '"
                        + DATA_IDS.get(3)
                        + "',"
                        + "  'historical.runtime-source' = 'NewSource',"
                        + "  'historical.bounded' = 'true',"
                        + "  'historical.readable-metadata' = 'metadata_1:STRING,metadata_2:INT',"
                        + "  'realtime.connector' = 'values',"
                        + "  'realtime.data-id' = '"
                        + DATA_IDS.get(4)
                        + "',"
                        + "  'realtime.runtime-source' = 'NewSource',"
                        + "  'realtime.readable-metadata' = 'metadata_1:STRING,metadata_3:INT'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT * FROM hybrid_source");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_d, flink_d, 9, meta0]\n"
                        + "+I[hello_d, hadoop_d, 10, meta1]\n"
                        + "+I[hello_d, world_d, 11, meta2]\n"
                        + "+I[hello_e, flink_e, 12, meta3]\n"
                        + "+I[hello_e, hadoop_e, 13, meta4]\n"
                        + "+I[hello_e, world_e, 14, meta5]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    @ParameterizedTest
    @MethodSource("envs")
    void tesHybridSourceOneSourceHasDDLMetaData(EnvMode envMode) {
        TableEnvironment tEnv = getTableEnv(envMode);
        // the ddl metadata field just belongs first source,
        // the second source without this metadata
        tEnv.executeSql(
                "CREATE TABLE hybrid_source("
                        + "  f0 varchar,"
                        + "  f1 varchar,"
                        + "  f2 bigint,"
                        + "  metadata_1 varchar metadata"
                        + ") WITH ("
                        + "  'connector' = 'hybrid',"
                        + "  'source-identifiers' = 'historical,realtime',"
                        + "  'historical.connector' = 'values',"
                        + "  'historical.data-id' = '"
                        + DATA_IDS.get(3)
                        + "',"
                        + "  'historical.runtime-source' = 'NewSource',"
                        + "  'historical.bounded' = 'true',"
                        + "  'historical.readable-metadata' = 'metadata_1:STRING,metadata_2:INT',"
                        + "  'realtime.connector' = 'values',"
                        + "  'realtime.data-id' = '"
                        + DATA_IDS.get(5)
                        + "',"
                        + "  'realtime.runtime-source' = 'NewSource',"
                        + "  'realtime.readable-metadata' = 'metadata_3:STRING,metadata_4:INT'"
                        + ")");

        Table table = tEnv.sqlQuery("SELECT * FROM hybrid_source");

        List<Row> results = CollectionUtil.iteratorToList(table.execute().collect());
        String expected =
                "+I[hello_d, flink_d, 9, meta0]\n"
                        + "+I[hello_d, hadoop_d, 10, meta1]\n"
                        + "+I[hello_d, world_d, 11, meta2]\n"
                        + "+I[hello_e, flink_e, 15, null]\n"
                        + "+I[hello_e, hadoop_e, 16, null]\n"
                        + "+I[hello_e, world_e, 17, null]\n";
        TestBaseUtils.compareResultAsText(results, expected);
    }

    private enum EnvMode {
        Streaming,
        Batch
    }
}
