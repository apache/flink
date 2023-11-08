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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.planner.utils.TableTestUtil.readFromResource;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceNodeIdInOperator;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStageId;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link HiveTableSink}. */
@ExtendWith(TestLoggerExtension.class)
class HiveTableSinkITCase {

    private static HiveCatalog hiveCatalog;

    @BeforeAll
    static void createCatalog() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterAll
    static void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    void testHiveTableSinkWithParallelismInBatch() throws Exception {
        final TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        testHiveTableSinkWithParallelismBase(
                tEnv, "/explain/testHiveTableSinkWithParallelismInBatch.out");
    }

    @Test
    void testHiveTableSinkWithParallelismInStreaming() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final TableEnvironment tEnv =
                HiveTestUtils.createTableEnvInStreamingMode(env, SqlDialect.HIVE);
        testHiveTableSinkWithParallelismBase(
                tEnv, "/explain/testHiveTableSinkWithParallelismInStreaming.out");
    }

    private void testHiveTableSinkWithParallelismBase(
            final TableEnvironment tEnv, final String expectedResourceFileName) throws Exception {
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        TableEnvExecutorUtil.executeInSeparateDatabase(
                tEnv,
                true,
                () -> {
                    tEnv.executeSql(
                            "CREATE TABLE test_table ("
                                    + " id int,"
                                    + " real_col int"
                                    + ") TBLPROPERTIES ("
                                    + " 'sink.parallelism' = '8'" // set sink parallelism = 8
                                    + ")");
                    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                    final String actual =
                            tEnv.explainSql(
                                    "insert into test_table select 1, 1",
                                    ExplainDetail.JSON_EXECUTION_PLAN);
                    final String expected = readFromResource(expectedResourceFileName);

                    assertThat(replaceNodeIdInOperator(replaceStreamNodeId(replaceStageId(actual))))
                            .isEqualTo(
                                    replaceNodeIdInOperator(
                                            replaceStreamNodeId(replaceStageId(expected))));
                });
    }

    @Test
    void testBatchAppend() throws Exception {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        TableEnvExecutorUtil.executeInSeparateDatabase(
                tEnv,
                true,
                () -> {
                    tEnv.executeSql("create table append_table (i int, j int)");
                    tEnv.executeSql("insert into append_table select 1, 1").await();
                    tEnv.executeSql("insert into append_table select 2, 2").await();
                    List<Row> rows =
                            CollectionUtil.iteratorToList(
                                    tEnv.executeSql("select * from append_table").collect());
                    rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
                    assertThat(rows).isEqualTo(Arrays.asList(Row.of(1, 1), Row.of(2, 2)));
                });
    }

    @Test
    void testDefaultSerPartStreamingWrite() throws Exception {
        testStreamingWrite(true, false, "textfile", this::checkSuccessFiles);
    }

    @Test
    void testPartStreamingWrite() throws Exception {
        testStreamingWrite(true, false, "parquet", this::checkSuccessFiles);
        // disable vector orc writer test for hive 2.x due to dependency conflict
        if (!hiveCatalog.getHiveVersion().startsWith("2.")) {
            testStreamingWrite(true, false, "orc", this::checkSuccessFiles);
        }
    }

    @Test
    void testNonPartStreamingWrite() throws Exception {
        testStreamingWrite(false, false, "parquet", (p) -> {});
        // disable vector orc writer test for hive 2.x due to dependency conflict
        if (!hiveCatalog.getHiveVersion().startsWith("2.")) {
            testStreamingWrite(false, false, "orc", (p) -> {});
        }
    }

    @Test
    void testPartStreamingMrWrite() throws Exception {
        testStreamingWrite(true, true, "parquet", this::checkSuccessFiles);
        // doesn't support writer 2.0 orc table
        if (!hiveCatalog.getHiveVersion().startsWith("2.0")) {
            testStreamingWrite(true, true, "orc", this::checkSuccessFiles);
        }
    }

    @Test
    void testNonPartStreamingMrWrite() throws Exception {
        testStreamingWrite(false, true, "parquet", (p) -> {});
        // doesn't support writer 2.0 orc table
        if (!hiveCatalog.getHiveVersion().startsWith("2.0")) {
            testStreamingWrite(false, true, "orc", (p) -> {});
        }
    }

    @Test
    void testStreamingAppend() throws Exception {
        testStreamingWrite(
                false,
                false,
                "parquet",
                (p) -> {
                    StreamExecutionEnvironment env =
                            StreamExecutionEnvironment.getExecutionEnvironment();
                    env.setParallelism(1);
                    StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvInStreamingMode(env);
                    tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
                    tEnv.useCatalog(hiveCatalog.getName());
                    assertThatCode(
                                    () ->
                                            tEnv.executeSql(
                                                            "insert into db1.sink_table select 6,'a','b','2020-05-03','12'")
                                                    .await())
                            .doesNotThrowAnyException();
                    assertBatch(
                            "db1.sink_table",
                            Arrays.asList(
                                    "+I[1, a, b, 2020-05-03, 7]",
                                    "+I[1, a, b, 2020-05-03, 7]",
                                    "+I[2, p, q, 2020-05-03, 8]",
                                    "+I[2, p, q, 2020-05-03, 8]",
                                    "+I[3, x, y, 2020-05-03, 9]",
                                    "+I[3, x, y, 2020-05-03, 9]",
                                    "+I[4, x, y, 2020-05-03, 10]",
                                    "+I[4, x, y, 2020-05-03, 10]",
                                    "+I[5, x, y, 2020-05-03, 11]",
                                    "+I[5, x, y, 2020-05-03, 11]",
                                    "+I[6, a, b, 2020-05-03, 12]"));
                });
    }

    @Test
    void testStreamingSinkWithTimestampLtzWatermark() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvInStreamingMode(env);

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        TableEnvExecutorUtil.executeInSeparateDatabase(
                tEnv,
                true,
                () -> {
                    // source table DDL
                    tEnv.executeSql(
                            "create external table source_table ("
                                    + " a int,"
                                    + " b string,"
                                    + " c string,"
                                    + " epoch_ts bigint)"
                                    + " partitioned by ("
                                    + " pt_day string, pt_hour string) TBLPROPERTIES("
                                    + "'partition.time-extractor.timestamp-pattern'='$pt_day $pt_hour:00:00',"
                                    + "'streaming-source.enable'='true',"
                                    + "'streaming-source.monitor-interval'='1s',"
                                    + "'streaming-source.consume-order'='partition-time'"
                                    + ")");

                    tEnv.executeSql(
                            "create external table sink_table ("
                                    + " a int,"
                                    + " b string,"
                                    + " c string)"
                                    + " partitioned by ("
                                    + " d string, e string) TBLPROPERTIES("
                                    + " 'partition.time-extractor.timestamp-pattern' = '$d $e:00:00',"
                                    + " 'auto-compaction'='true',"
                                    + " 'compaction.file-size' = '128MB',"
                                    + " 'sink.partition-commit.trigger'='partition-time',"
                                    + " 'sink.partition-commit.delay'='30min',"
                                    + " 'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',"
                                    + " 'sink.partition-commit.policy.kind'='metastore,success-file',"
                                    + " 'sink.partition-commit.success-file.name'='_MY_SUCCESS',"
                                    + " 'streaming-source.enable'='true',"
                                    + " 'streaming-source.monitor-interval'='1s',"
                                    + " 'streaming-source.consume-order'='partition-time'"
                                    + ")");

                    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

                    // Build a partitioned table source with watermark base on the streaming-hive
                    // table
                    DataStream<Row> dataStream =
                            tEnv.toDataStream(
                                    tEnv.sqlQuery(
                                            "select a, b, c, epoch_ts, pt_day, pt_hour from source_table"));
                    Table table =
                            tEnv.fromDataStream(
                                    dataStream,
                                    Schema.newBuilder()
                                            .column("a", DataTypes.INT())
                                            .column("b", DataTypes.STRING())
                                            .column("c", DataTypes.STRING())
                                            .column("epoch_ts", DataTypes.BIGINT())
                                            .column("pt_day", DataTypes.STRING())
                                            .column("pt_hour", DataTypes.STRING())
                                            .columnByExpression(
                                                    "ts_ltz",
                                                    Expressions.callSql(
                                                            "TO_TIMESTAMP_LTZ(epoch_ts, 3)"))
                                            .watermark("ts_ltz", "ts_ltz - INTERVAL '1' SECOND")
                                            .build());
                    tEnv.createTemporaryView("my_table", table);
                    /*
                     * prepare test data, we write two records into each partition in source table
                     * the epoch mills used to define watermark, the watermark value is
                     * the max timestamp value of all the partition data, i.e:
                     * partition timestamp + 1 hour - 1 second in this case
                     *
                     * <pre>
                     * epoch mills 1588461300000L <=>  local timestamp 2020-05-03 07:15:00 in Shanghai
                     * epoch mills 1588463100000L <=>  local timestamp 2020-05-03 07:45:00 in Shanghai
                     * epoch mills 1588464300000L <=>  local timestamp 2020-05-03 08:05:00 in Shanghai
                     * epoch mills 1588466400000L <=>  local timestamp 2020-05-03 08:40:00 in Shanghai
                     * epoch mills 1588468800000L <=>  local timestamp 2020-05-03 09:20:00 in Shanghai
                     * epoch mills 1588470900000L <=>  local timestamp 2020-05-03 09:55:00 in Shanghai
                     * epoch mills 1588471800000L <=>  local timestamp 2020-05-03 10:10:00 in Shanghai
                     * epoch mills 1588473300000L <=>  local timestamp 2020-05-03 10:35:00 in Shanghai
                     * epoch mills 1588476300000L <=>  local timestamp 2020-05-03 11:25:00 in Shanghai
                     * epoch mills 1588477800000L <=>  local timestamp 2020-05-03 11:50:00 in Shanghai
                     * </pre>
                     */
                    Map<Integer, Object[]> testData = new HashMap<>();
                    testData.put(1, new Object[] {1, "a", "b", 1588461300000L});
                    testData.put(2, new Object[] {1, "a", "b", 1588463100000L});
                    testData.put(3, new Object[] {2, "p", "q", 1588464300000L});
                    testData.put(4, new Object[] {2, "p", "q", 1588466400000L});
                    testData.put(5, new Object[] {3, "x", "y", 1588468800000L});
                    testData.put(6, new Object[] {3, "x", "y", 1588470900000L});
                    testData.put(7, new Object[] {4, "x", "y", 1588471800000L});
                    testData.put(8, new Object[] {4, "x", "y", 1588473300000L});
                    testData.put(9, new Object[] {5, "x", "y", 1588476300000L});
                    testData.put(10, new Object[] {5, "x", "y", 1588477800000L});

                    Map<Integer, String> testPartition = new HashMap<>();
                    testPartition.put(1, "pt_day='2020-05-03',pt_hour='7'");
                    testPartition.put(2, "pt_day='2020-05-03',pt_hour='8'");
                    testPartition.put(3, "pt_day='2020-05-03',pt_hour='9'");
                    testPartition.put(4, "pt_day='2020-05-03',pt_hour='10'");
                    testPartition.put(5, "pt_day='2020-05-03',pt_hour='11'");

                    Map<Integer, Object[]> expectedData = new HashMap<>();
                    expectedData.put(1, new Object[] {1, "a", "b", "2020-05-03", "7"});
                    expectedData.put(2, new Object[] {2, "p", "q", "2020-05-03", "8"});
                    expectedData.put(3, new Object[] {3, "x", "y", "2020-05-03", "9"});
                    expectedData.put(4, new Object[] {4, "x", "y", "2020-05-03", "10"});
                    expectedData.put(5, new Object[] {5, "x", "y", "2020-05-03", "11"});

                    tEnv.executeSql(
                            "insert into sink_table select a, b, c, pt_day, pt_hour from my_table");
                    CloseableIterator<Row> iter =
                            tEnv.executeSql("select * from sink_table").collect();

                    HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "source_table")
                            .addRow(testData.get(1))
                            .addRow(testData.get(2))
                            .commit(testPartition.get(1));

                    for (int i = 2; i < 7; i++) {
                        try {
                            Thread.sleep(1_000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        assertThat(fetchRows(iter, 2))
                                .isEqualTo(
                                        Arrays.asList(
                                                Row.of(expectedData.get(i - 1)).toString(),
                                                Row.of(expectedData.get(i - 1)).toString()));

                        if (i < 6) {
                            HiveTestUtils.createTextTableInserter(
                                            hiveCatalog, "db1", "source_table")
                                    .addRow(testData.get(2 * i - 1))
                                    .addRow(testData.get(2 * i))
                                    .commit(testPartition.get(i));
                        }
                    }
                    checkSuccessFiles(
                            URI.create(
                                            hiveCatalog
                                                    .getHiveTable(
                                                            ObjectPath.fromString("db1.sink_table"))
                                                    .getSd()
                                                    .getLocation())
                                    .getPath());
                });
    }

    @Test
    void testStreamingSinkWithoutCommitPolicy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = HiveTestUtils.createTableEnvInStreamingMode(env);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());

        TableEnvExecutorUtil.executeInSeparateDatabase(
                tableEnv,
                true,
                () -> {
                    tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
                    tableEnv.executeSql("create table dest(x int) partitioned by (p string)");

                    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                    tableEnv.executeSql(
                            "create table src (i int, p string) with ("
                                    + "'connector'='datagen',"
                                    + "'number-of-rows'='5')");

                    assertThatThrownBy(
                                    () ->
                                            tableEnv.executeSql(
                                                            "insert into dest select * from src")
                                                    .await(),
                                    "Streaming write partitioned table without commit policy should fail")
                            .isInstanceOf(FlinkHiveException.class)
                            .hasMessageContaining(
                                    String.format(
                                            "Streaming write to partitioned hive table `%s`.`%s`.`%s` without providing a commit policy",
                                            hiveCatalog.getName(), "db1", "dest"));
                });
    }

    @Test
    void testCustomPartitionCommitPolicyNotFound() {
        String customCommitPolicyClassName = "NotExistPartitionCommitPolicyClass";

        assertThatThrownBy(
                        () ->
                                testStreamingWriteWithCustomPartitionCommitPolicy(
                                        customCommitPolicyClassName))
                .hasStackTraceContaining(
                        "Can not create new instance for custom class from "
                                + customCommitPolicyClassName);
    }

    @Test
    void testCustomPartitionCommitPolicy() throws Exception {
        testStreamingWriteWithCustomPartitionCommitPolicy(TestCustomCommitPolicy.class.getName());
    }

    @Test
    void testWritingNoDataToPartition() throws Exception {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.executeSql("CREATE TABLE src_table (name string) PARTITIONED BY (`dt` string)");
        tEnv.executeSql("CREATE TABLE target_table (name string) PARTITIONED BY (`dt` string)");

        // insert into partition
        tEnv.executeSql(
                        "INSERT INTO target_table partition (dt='2022-07-27') SELECT name FROM src_table where dt = '2022-07-27'")
                .await();
        List<Row> partitions =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("show partitions target_table").collect());
        assertThat(partitions).hasSize(1);
        assertThat(partitions.toString()).contains("dt=2022-07-27");

        // insert overwrite partition
        tEnv.executeSql(
                        "INSERT OVERWRITE TABLE target_table partition (dt='2022-07-28') SELECT name FROM src_table where dt = '2022-07-28'")
                .await();
        partitions =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("show partitions target_table").collect());
        assertThat(partitions).hasSize(2);
        assertThat(partitions.toString()).contains("dt=2022-07-28");

        // insert into a partition with data
        tEnv.executeSql("INSERT INTO target_table partition (dt='2022-07-29') VALUES ('zm')")
                .await();

        assertBatch("target_table", Collections.singletonList("+I[zm, 2022-07-29]"));
        tEnv.executeSql(
                        "INSERT INTO target_table partition (dt='2022-07-29') SELECT name FROM src_table where dt = '2022-07-29'")
                .await();
        partitions =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("show partitions target_table").collect());
        assertThat(partitions).hasSize(3);
        assertThat(partitions.toString()).contains("dt=2022-07-29");
        assertBatch("target_table", Collections.singletonList("+I[zm, 2022-07-29]"));

        // insert overwrite a partition with data
        tEnv.executeSql(
                        "INSERT OVERWRITE TABLE target_table partition (dt='2022-07-29') SELECT name FROM src_table where dt = '2022-07-29'")
                .await();
        partitions =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("show partitions target_table").collect());
        assertThat(partitions).hasSize(3);
        assertThat(partitions.toString()).contains("dt=2022-07-29");
        assertBatch("target_table", Collections.emptyList());

        // test for dynamic partition
        tEnv.executeSql(
                "create table partition_table(`name` string) partitioned by (`p_date` string, `p_hour` string)");
        tEnv.executeSql("create table test_src_table(`name` string, `hour` string, age int)");
        tEnv.executeSql(
                        "insert overwrite table partition_table partition(`p_date`='20220816', `p_hour`)"
                                + " select `name`, `hour` from test_src_table")
                .await();
        partitions =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql("show partitions partition_table").collect());
        assertThat(partitions).hasSize(0);
    }

    @Test
    void testSortByDynamicPartitionEnableConfigurationInBatchMode() {
        final TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode();
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        try {
            // sort by dynamic partition columns is enabled by default
            tEnv.executeSql(
                    String.format(
                            "create table dynamic_partition_t(a int, b int, d string)"
                                    + " partitioned by (d) with ('connector' = 'hive', '%s' = 'metastore')",
                            SINK_PARTITION_COMMIT_POLICY_KIND.key()));
            String actual = tEnv.explainSql("insert into dynamic_partition_t select 1, 1, 'd'");
            assertThat(actual)
                    .isEqualTo(readFromResource("/explain/testDynamicPartitionSortEnabled.out"));

            // disable sorting
            tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_DYNAMIC_GROUPING_ENABLED, false);
            actual = tEnv.explainSql("insert into dynamic_partition_t select 1, 1, 'd'");
            assertThat(actual)
                    .isEqualTo(readFromResource("/explain/testDynamicPartitionSortDisabled.out"));
        } finally {
            tEnv.executeSql("drop table dynamic_partition_t");
        }
    }

    @Test
    void testWriteSuccessFile() throws Exception {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());

        String successFileName = tEnv.getConfig().get(SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
        String warehouse =
                hiveCatalog.getHiveConf().get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);

        tEnv.executeSql("CREATE TABLE zm_test_non_partition_table (name string)");
        tEnv.executeSql(
                "CREATE TABLE zm_test_partition_table (name string) PARTITIONED BY (`dt` string)");

        // test partition table
        String partitionTablePath = warehouse + "/zm_test_partition_table";

        tEnv.executeSql(
                        "INSERT INTO zm_test_partition_table partition (dt='2022-07-27') values ('zm')")
                .await();
        assertThat(new File(partitionTablePath, "dt=2022-07-27/" + successFileName)).exists();

        // test non-partition table
        String nonPartitionTablePath = warehouse + "/zm_test_non_partition_table";
        tEnv.executeSql("INSERT INTO zm_test_non_partition_table values ('zm')").await();
        assertThat(new File(nonPartitionTablePath, successFileName)).exists();

        // test only metastore policy
        tEnv.executeSql(
                "CREATE TABLE zm_test_partition_table_only_meta (name string) "
                        + "PARTITIONED BY (`dt` string) "
                        + "TBLPROPERTIES ('sink.partition-commit.policy.kind' = 'metastore')");
        String onlyMetaTablePath = warehouse + "/zm_test_partition_table_only_meta";

        tEnv.executeSql(
                        "INSERT INTO zm_test_partition_table_only_meta partition (dt='2022-08-15') values ('zm')")
                .await();
        assertThat(new File(onlyMetaTablePath, "dt=2022-08-15/" + successFileName)).doesNotExist();

        // test change success file name
        tEnv.executeSql(
                "CREATE TABLE zm_test_partition_table_success_file (name string) "
                        + "PARTITIONED BY (`dt` string) "
                        + "TBLPROPERTIES ('sink.partition-commit.success-file.name' = '_ZM')");
        String changeFileNameTablePath = warehouse + "/zm_test_partition_table_success_file";
        tEnv.executeSql(
                        "INSERT INTO zm_test_partition_table_success_file partition (dt='2022-08-15') values ('zm')")
                .await();
        assertThat(new File(changeFileNameTablePath, "dt=2022-08-15/" + successFileName))
                .doesNotExist();
        assertThat(new File(changeFileNameTablePath, "dt=2022-08-15/_ZM")).exists();
    }

    @Test
    void testAutoGatherStatisticForBatchWriting() throws Exception {
        TableEnvironment tEnv = HiveTestUtils.createTableEnvInBatchMode(SqlDialect.HIVE);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        String wareHouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        // disable auto statistic first
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE, false);
        // test non-partition table
        tEnv.executeSql("create table t1(x int)");
        tEnv.executeSql("create table t2(x int) stored as orc");
        tEnv.executeSql("create table t3(x int) stored as parquet");
        tEnv.executeSql("insert into t1 values (1)").await();
        tEnv.executeSql("insert into t2 values (1)").await();
        tEnv.executeSql("insert into t3 values (1)").await();
        // check the statistic for these table
        // the statistics should be empty since the auto gather statistic is disabled
        for (int i = 1; i <= 3; i++) {
            CatalogTableStatistics statistics =
                    hiveCatalog.getTableStatistics(new ObjectPath("default", "t" + i));
            assertThat(statistics).isEqualTo(CatalogTableStatistics.UNKNOWN);
        }
        // now enable auto gather statistic
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE, true);
        tEnv.executeSql("insert into t1 values (1)").await();
        tEnv.executeSql("insert into t2 values (1)").await();
        tEnv.executeSql("insert into t3 values (1)").await();
        CatalogTableStatistics statistics =
                hiveCatalog.getTableStatistics(new ObjectPath("default", "t1"));
        // t1 is neither stored as orc nor parquet, so only fileCount and totalSize is
        // calculated
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                -1, 2, getPathSize(Paths.get(wareHouse, "t1")), -1));
        statistics = hiveCatalog.getTableStatistics(new ObjectPath("default", "t2"));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                2, 2, getPathSize(Paths.get(wareHouse, "t2")), 8));
        statistics = hiveCatalog.getTableStatistics(new ObjectPath("default", "t3"));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                2, 2, getPathSize(Paths.get(wareHouse, "t3")), 66));

        // test partition table
        tEnv.executeSql("create table pt1(x int) partitioned by (y int)");
        tEnv.executeSql("create table pt2(x int) partitioned by (y int) stored as orc");
        tEnv.executeSql("create table pt3(x int) partitioned by (y int) stored as parquet");
        tEnv.executeSql("insert into pt1 partition(y=1) values (1)").await();
        tEnv.executeSql("insert into pt2 partition(y=2) values (2)").await();
        tEnv.executeSql("insert into pt3 partition(y=3) values (3)").await();

        // verify statistic
        statistics =
                hiveCatalog.getPartitionStatistics(
                        new ObjectPath("default", "pt1"),
                        new CatalogPartitionSpec(Collections.singletonMap("y", "1")));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                -1, 1, getPathSize(Paths.get(wareHouse, "pt1", "y=1")), -1));
        statistics =
                hiveCatalog.getPartitionStatistics(
                        new ObjectPath("default", "pt2"),
                        new CatalogPartitionSpec(Collections.singletonMap("y", "2")));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                1, 1, getPathSize(Paths.get(wareHouse, "pt2", "y=2")), 4));
        statistics =
                hiveCatalog.getPartitionStatistics(
                        new ObjectPath("default", "pt3"),
                        new CatalogPartitionSpec(Collections.singletonMap("y", "3")));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                1, 1, getPathSize(Paths.get(wareHouse, "pt3", "y=3")), 33));

        // insert data into partition again
        tEnv.executeSql("insert into pt1 partition(y=1) values (1)").await();
        tEnv.executeSql("insert into pt2 partition(y=2) values (2)").await();
        tEnv.executeSql("insert into pt3 partition(y=3) values (3)").await();

        // verify statistic
        statistics =
                hiveCatalog.getPartitionStatistics(
                        new ObjectPath("default", "pt1"),
                        new CatalogPartitionSpec(Collections.singletonMap("y", "1")));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                -1, 2, getPathSize(Paths.get(wareHouse, "pt1", "y=1")), -1));

        statistics =
                hiveCatalog.getPartitionStatistics(
                        new ObjectPath("default", "pt2"),
                        new CatalogPartitionSpec(Collections.singletonMap("y", "2")));

        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                2, 2, getPathSize(Paths.get(wareHouse, "pt2", "y=2")), 8));

        statistics =
                hiveCatalog.getPartitionStatistics(
                        new ObjectPath("default", "pt3"),
                        new CatalogPartitionSpec(Collections.singletonMap("y", "3")));
        assertThat(statistics)
                .isEqualTo(
                        new CatalogTableStatistics(
                                2, 2, getPathSize(Paths.get(wareHouse, "pt3", "y=3")), 66));

        // test overwrite table/partition
        tEnv.executeSql("create table src(x int)");
        tEnv.executeSql("insert overwrite table pt1 partition(y=1) select * from src").await();
        tEnv.executeSql("insert overwrite table pt2 partition(y=2) select * from src").await();
        tEnv.executeSql("insert overwrite table pt3 partition(y=3) select * from src").await();

        for (int i = 1; i <= 3; i++) {
            statistics =
                    hiveCatalog.getPartitionStatistics(
                            new ObjectPath("default", "pt" + i),
                            new CatalogPartitionSpec(
                                    Collections.singletonMap("y", String.valueOf(i))));
            assertThat(statistics).isEqualTo(CatalogTableStatistics.UNKNOWN);
        }
    }

    private long getPathSize(java.nio.file.Path path) throws IOException {
        String defaultSuccessFileName =
                FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.defaultValue();
        return Files.list(path)
                .filter(
                        p ->
                                !p.toFile().isHidden()
                                        && !p.toFile().getPath().equals(defaultSuccessFileName))
                .map(p -> p.toFile().length())
                .reduce(Long::sum)
                .orElse(0L);
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            assertThat(iter.hasNext()).isTrue();
            strings.add(iter.next().toString());
        }
        strings.sort(String::compareTo);
        return strings;
    }

    private void checkSuccessFiles(String path) {
        File basePath = new File(path, "d=2020-05-03");
        assertThat(basePath.list()).hasSize(5);
        assertThat(new File(new File(basePath, "e=7"), "_MY_SUCCESS").exists()).isTrue();
        assertThat(new File(new File(basePath, "e=8"), "_MY_SUCCESS").exists()).isTrue();
        assertThat(new File(new File(basePath, "e=9"), "_MY_SUCCESS").exists()).isTrue();
        assertThat(new File(new File(basePath, "e=10"), "_MY_SUCCESS").exists()).isTrue();
        assertThat(new File(new File(basePath, "e=11"), "_MY_SUCCESS").exists()).isTrue();
    }

    private void testStreamingWriteWithCustomPartitionCommitPolicy(
            String customPartitionCommitPolicyClassName) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        // avoid the job to restart infinitely
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1_000));

        StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvInStreamingMode(env);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        TableEnvExecutorUtil.executeInSeparateDatabase(
                tEnv,
                true,
                () -> {
                    // prepare source
                    List<Row> data =
                            Arrays.asList(
                                    Row.of(1, "a", "b", "2020-05-03", "7"),
                                    Row.of(2, "p", "q", "2020-05-03", "8"),
                                    Row.of(3, "x", "y", "2020-05-03", "9"),
                                    Row.of(4, "x", "y", "2020-05-03", "10"),
                                    Row.of(5, "x", "y", "2020-05-03", "11"));
                    DataStream<Row> stream =
                            env.addSource(
                                    new FiniteTestSource<>(data),
                                    new RowTypeInfo(
                                            Types.INT,
                                            Types.STRING,
                                            Types.STRING,
                                            Types.STRING,
                                            Types.STRING));
                    tEnv.createTemporaryView(
                            "my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"));

                    // DDL
                    tEnv.executeSql(
                            "create external table sink_table (a int,b string,c string"
                                    + ") "
                                    + "partitioned by (d string,e string) "
                                    + " stored as textfile"
                                    + " TBLPROPERTIES ("
                                    + "'"
                                    + SINK_PARTITION_COMMIT_DELAY.key()
                                    + "'='1h',"
                                    + "'"
                                    + SINK_PARTITION_COMMIT_POLICY_KIND.key()
                                    + "'='metastore,custom',"
                                    + "'"
                                    + SINK_PARTITION_COMMIT_POLICY_CLASS.key()
                                    + "'='"
                                    + customPartitionCommitPolicyClassName
                                    + "'"
                                    + ")");

                    // hive dialect only works with hive tables at the moment, switch to default
                    // dialect
                    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                    tEnv.sqlQuery("select * from my_table").executeInsert("sink_table").await();

                    // check committed partitions for CustomizedCommitPolicy
                    Set<String> committedPaths =
                            TestCustomCommitPolicy.getCommittedPartitionPathsAndReset();
                    String base =
                            URI.create(
                                            hiveCatalog
                                                    .getHiveTable(
                                                            ObjectPath.fromString("db1.sink_table"))
                                                    .getSd()
                                                    .getLocation())
                                    .getPath();
                    List<String> partitionKVs =
                            Lists.newArrayList("e=7", "e=8", "e=9", "e=10", "e=11");
                    partitionKVs.forEach(
                            partitionKV -> {
                                String partitionPath =
                                        new Path(new Path(base, "d=2020-05-03"), partitionKV)
                                                .toString();
                                assertThat(committedPaths)
                                        .as(
                                                "Partition(d=2020-05-03, "
                                                        + partitionKV
                                                        + ") is not committed successfully")
                                        .contains(partitionPath);
                            });
                });
    }

    private void testStreamingWrite(
            boolean part, boolean useMr, String format, Consumer<String> pathConsumer)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvInStreamingMode(env);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        if (useMr) {
            tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, true);
        } else {
            tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, false);
        }

        TableEnvExecutorUtil.executeInSeparateDatabase(
                tEnv,
                true,
                () -> {
                    // prepare source
                    List<Row> data =
                            Arrays.asList(
                                    Row.of(1, "a", "b", "2020-05-03", "7"),
                                    Row.of(2, "p", "q", "2020-05-03", "8"),
                                    Row.of(3, "x", "y", "2020-05-03", "9"),
                                    Row.of(4, "x", "y", "2020-05-03", "10"),
                                    Row.of(5, "x", "y", "2020-05-03", "11"));
                    DataStream<Row> stream =
                            env.addSource(
                                    new FiniteTestSource<>(data),
                                    new RowTypeInfo(
                                            Types.INT,
                                            Types.STRING,
                                            Types.STRING,
                                            Types.STRING,
                                            Types.STRING));
                    tEnv.createTemporaryView(
                            "my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"));

                    // DDL
                    tEnv.executeSql(
                            "create external table sink_table (a int,b string,c string"
                                    + (part ? "" : ",d string,e string")
                                    + ") "
                                    + (part ? "partitioned by (d string,e string) " : "")
                                    + " stored as "
                                    + format
                                    + " TBLPROPERTIES ("
                                    + "'"
                                    + PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()
                                    + "'='$d $e:00:00',"
                                    + "'"
                                    + SINK_PARTITION_COMMIT_DELAY.key()
                                    + "'='1h',"
                                    + "'"
                                    + SINK_PARTITION_COMMIT_POLICY_KIND.key()
                                    + "'='metastore,success-file',"
                                    + "'"
                                    + SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key()
                                    + "'='_MY_SUCCESS'"
                                    + ")");

                    // hive dialect only works with hive tables at the moment, switch to default
                    // dialect
                    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
                    tEnv.sqlQuery("select * from my_table").executeInsert("sink_table").await();

                    assertBatch(
                            "db1.sink_table",
                            Arrays.asList(
                                    "+I[1, a, b, 2020-05-03, 7]",
                                    "+I[1, a, b, 2020-05-03, 7]",
                                    "+I[2, p, q, 2020-05-03, 8]",
                                    "+I[2, p, q, 2020-05-03, 8]",
                                    "+I[3, x, y, 2020-05-03, 9]",
                                    "+I[3, x, y, 2020-05-03, 9]",
                                    "+I[4, x, y, 2020-05-03, 10]",
                                    "+I[4, x, y, 2020-05-03, 10]",
                                    "+I[5, x, y, 2020-05-03, 11]",
                                    "+I[5, x, y, 2020-05-03, 11]"));

                    pathConsumer.accept(
                            URI.create(
                                            hiveCatalog
                                                    .getHiveTable(
                                                            ObjectPath.fromString("db1.sink_table"))
                                                    .getSd()
                                                    .getLocation())
                                    .getPath());
                });
    }

    private void assertBatch(String table, List<String> expected) {
        // using batch table env to query.
        List<String> results = new ArrayList<>();
        TableEnvironment batchTEnv = HiveTestUtils.createTableEnvInBatchMode();
        batchTEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchTEnv.useCatalog(hiveCatalog.getName());
        batchTEnv
                .executeSql("select * from " + table)
                .collect()
                .forEachRemaining(r -> results.add(r.toString()));
        results.sort(String::compareTo);
        expected.sort(String::compareTo);
        assertThat(results).isEqualTo(expected);
    }
}
