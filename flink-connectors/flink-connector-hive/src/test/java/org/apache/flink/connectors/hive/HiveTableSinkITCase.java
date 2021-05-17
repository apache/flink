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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
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
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME;
import static org.apache.flink.table.planner.utils.TableTestUtil.readFromResource;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStageId;
import static org.apache.flink.table.planner.utils.TableTestUtil.replaceStreamNodeId;
import static org.junit.Assert.assertEquals;

/** Tests {@link HiveTableSink}. */
public class HiveTableSinkITCase {

    private static HiveCatalog hiveCatalog;

    @BeforeClass
    public static void createCatalog() throws IOException {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();
    }

    @AfterClass
    public static void closeCatalog() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
    }

    @Test
    public void testHiveTableSinkWithParallelismInBatch() {
        final TableEnvironment tEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
        testHiveTableSinkWithParallelismBase(
                tEnv, "/explain/testHiveTableSinkWithParallelismInBatch.out");
    }

    @Test
    public void testHiveTableSinkWithParallelismInStreaming() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final TableEnvironment tEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env, SqlDialect.HIVE);
        testHiveTableSinkWithParallelismBase(
                tEnv, "/explain/testHiveTableSinkWithParallelismInStreaming.out");
    }

    private void testHiveTableSinkWithParallelismBase(
            final TableEnvironment tEnv, final String expectedResourceFileName) {
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.executeSql("create database db1");
        tEnv.useDatabase("db1");

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE test_table ("
                                + " id int,"
                                + " real_col int"
                                + ") TBLPROPERTIES ("
                                + " 'sink.parallelism' = '8'" // set sink parallelism = 8
                                + ")"));
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        final String actual =
                tEnv.explainSql(
                        "insert into test_table select 1, 1", ExplainDetail.JSON_EXECUTION_PLAN);
        final String expected = readFromResource(expectedResourceFileName);

        assertEquals(
                replaceStreamNodeId(replaceStageId(expected)),
                replaceStreamNodeId(replaceStageId(actual)));

        tEnv.executeSql("drop database db1 cascade");
    }

    @Test
    public void testBatchAppend() throws Exception {
        TableEnvironment tEnv =
                HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.executeSql("create database db1");
        tEnv.useDatabase("db1");
        try {
            tEnv.executeSql("create table append_table (i int, j int)");
            tEnv.executeSql("insert into append_table select 1, 1").await();
            tEnv.executeSql("insert into append_table select 2, 2").await();
            List<Row> rows =
                    CollectionUtil.iteratorToList(
                            tEnv.executeSql("select * from append_table").collect());
            rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
            Assert.assertEquals(Arrays.asList(Row.of(1, 1), Row.of(2, 2)), rows);
        } finally {
            tEnv.executeSql("drop database db1 cascade");
        }
    }

    @Test(timeout = 120000)
    public void testDefaultSerPartStreamingWrite() throws Exception {
        testStreamingWrite(true, false, "textfile", this::checkSuccessFiles);
    }

    @Test(timeout = 120000)
    public void testPartStreamingWrite() throws Exception {
        testStreamingWrite(true, false, "parquet", this::checkSuccessFiles);
        // disable vector orc writer test for hive 2.x due to dependency conflict
        if (!hiveCatalog.getHiveVersion().startsWith("2.")) {
            testStreamingWrite(true, false, "orc", this::checkSuccessFiles);
        }
    }

    @Test(timeout = 120000)
    public void testNonPartStreamingWrite() throws Exception {
        testStreamingWrite(false, false, "parquet", (p) -> {});
        // disable vector orc writer test for hive 2.x due to dependency conflict
        if (!hiveCatalog.getHiveVersion().startsWith("2.")) {
            testStreamingWrite(false, false, "orc", (p) -> {});
        }
    }

    @Test(timeout = 120000)
    public void testPartStreamingMrWrite() throws Exception {
        testStreamingWrite(true, true, "parquet", this::checkSuccessFiles);
        // doesn't support writer 2.0 orc table
        if (!hiveCatalog.getHiveVersion().startsWith("2.0")) {
            testStreamingWrite(true, true, "orc", this::checkSuccessFiles);
        }
    }

    @Test(timeout = 120000)
    public void testNonPartStreamingMrWrite() throws Exception {
        testStreamingWrite(false, true, "parquet", (p) -> {});
        // doesn't support writer 2.0 orc table
        if (!hiveCatalog.getHiveVersion().startsWith("2.0")) {
            testStreamingWrite(false, true, "orc", (p) -> {});
        }
    }

    @Test(timeout = 120000)
    public void testStreamingAppend() throws Exception {
        testStreamingWrite(
                false,
                false,
                "parquet",
                (p) -> {
                    StreamExecutionEnvironment env =
                            StreamExecutionEnvironment.getExecutionEnvironment();
                    env.setParallelism(1);
                    StreamTableEnvironment tEnv =
                            HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env);
                    tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
                    tEnv.useCatalog(hiveCatalog.getName());

                    try {
                        tEnv.executeSql(
                                        "insert into db1.sink_table select 6,'a','b','2020-05-03','12'")
                                .await();
                    } catch (Exception e) {
                        Assert.fail("Failed to execute sql: " + e.getMessage());
                    }

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

    @Test(timeout = 120000)
    public void testStreamingSinkWithTimestampLtzWatermark() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);
        StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env);

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        try {
            tEnv.executeSql("create database db1");
            tEnv.useDatabase("db1");

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

            // Build a partitioned table source with watermark base on the streaming-hive table
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
                                            Expressions.callSql("TO_TIMESTAMP_LTZ(epoch_ts, 3)"))
                                    .watermark("ts_ltz", "ts_ltz - INTERVAL '1' SECOND")
                                    .build());
            tEnv.createTemporaryView("my_table", table);
            /*
             * prepare test data, the poch mills used to define watermark, the watermark value is
             * the max timestamp value of all the partition data, i.e:
             * partition timestamp + 1 hour - 1 second in this case
             *
             * <pre>
             * epoch mills 1588464000000L <=>  local timestamp 2020-05-03 08:00:00 in Shanghai
             * epoch mills 1588467600000L <=>  local timestamp 2020-05-03 09:00:00 in Shanghai
             * epoch mills 1588471200000L <=>  local timestamp 2020-05-03 10:00:00 in Shanghai
             * epoch mills 1588474800000L <=>  local timestamp 2020-05-03 11:00:00 in Shanghai
             * epoch mills 1588478400000L <=>  local timestamp 2020-05-03 12:00:00 in Shanghai
             * </pre>
             */
            Map<Integer, Object[]> testData = new HashMap<>();
            testData.put(1, new Object[] {1, "a", "b", 1588464000000L});
            testData.put(2, new Object[] {2, "p", "q", 1588467600000L});
            testData.put(3, new Object[] {3, "x", "y", 1588471200000L});
            testData.put(4, new Object[] {4, "x", "y", 1588474800000L});
            testData.put(5, new Object[] {5, "x", "y", 1588478400000L});

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

            tEnv.executeSql("insert into sink_table select a, b, c, pt_day, pt_hour from my_table");
            CloseableIterator<Row> iter = tEnv.executeSql("select * from sink_table").collect();

            HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "source_table")
                    .addRow(testData.get(1))
                    .addRow(testData.get(1))
                    .commit(testPartition.get(1));

            for (int i = 2; i < 7; i++) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Assert.assertEquals(
                        Arrays.asList(
                                Row.of(expectedData.get(i - 1)).toString(),
                                Row.of(expectedData.get(i - 1)).toString()),
                        fetchRows(iter, 2));

                if (i < 6) {
                    HiveTestUtils.createTextTableInserter(hiveCatalog, "db1", "source_table")
                            .addRow(testData.get(i))
                            .addRow(testData.get(i))
                            .commit(testPartition.get(i));
                }
            }
            this.checkSuccessFiles(
                    URI.create(
                                    hiveCatalog
                                            .getHiveTable(ObjectPath.fromString("db1.sink_table"))
                                            .getSd()
                                            .getLocation())
                            .getPath());
        } finally {
            tEnv.executeSql("drop database db1 cascade");
        }
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Assert.assertTrue(iter.hasNext());
            strings.add(iter.next().toString());
        }
        strings.sort(String::compareTo);
        return strings;
    }

    private void checkSuccessFiles(String path) {
        File basePath = new File(path, "d=2020-05-03");
        Assert.assertEquals(5, basePath.list().length);
        Assert.assertTrue(new File(new File(basePath, "e=7"), "_MY_SUCCESS").exists());
        Assert.assertTrue(new File(new File(basePath, "e=8"), "_MY_SUCCESS").exists());
        Assert.assertTrue(new File(new File(basePath, "e=9"), "_MY_SUCCESS").exists());
        Assert.assertTrue(new File(new File(basePath, "e=10"), "_MY_SUCCESS").exists());
        Assert.assertTrue(new File(new File(basePath, "e=11"), "_MY_SUCCESS").exists());
    }

    private void testStreamingWrite(
            boolean part, boolean useMr, String format, Consumer<String> pathConsumer)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100);

        StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env);
        tEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tEnv.useCatalog(hiveCatalog.getName());
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        if (useMr) {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, true);
        } else {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER, false);
        }

        try {
            tEnv.executeSql("create database db1");
            tEnv.useDatabase("db1");

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
            tEnv.createTemporaryView("my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"));

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

            // hive dialect only works with hive tables at the moment, switch to default dialect
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
                                            .getHiveTable(ObjectPath.fromString("db1.sink_table"))
                                            .getSd()
                                            .getLocation())
                            .getPath());
        } finally {
            tEnv.executeSql("drop database db1 cascade");
        }
    }

    private void assertBatch(String table, List<String> expected) {
        // using batch table env to query.
        List<String> results = new ArrayList<>();
        TableEnvironment batchTEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
        batchTEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        batchTEnv.useCatalog(hiveCatalog.getName());
        batchTEnv
                .executeSql("select * from " + table)
                .collect()
                .forEachRemaining(r -> results.add(r.toString()));
        results.sort(String::compareTo);
        expected.sort(String::compareTo);
        Assert.assertEquals(expected, results);
    }
}
