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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.internal.JdbcTableOutputFormatTest.check;
import static org.apache.flink.table.api.Expressions.$;

/** The ITCase for {@link JdbcDynamicTableSink}. */
public class JdbcDynamicTableSinkITCase extends AbstractTestBase {

    public static final String DB_URL = "jdbc:derby:memory:upsert";
    public static final String OUTPUT_TABLE1 = "dynamicSinkForUpsert";
    public static final String OUTPUT_TABLE2 = "dynamicSinkForAppend";
    public static final String OUTPUT_TABLE3 = "dynamicSinkForBatch";
    public static final String OUTPUT_TABLE4 = "REAL_TABLE";
    public static final String OUTPUT_TABLE5 = "checkpointTable";
    public static final String USER_TABLE = "USER_TABLE";

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        System.setProperty(
                "derby.stream.error.field", JdbcTestFixture.class.getCanonicalName() + ".DEV_NULL");

        Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
        try (Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
                Statement stat = conn.createStatement()) {
            stat.executeUpdate(
                    "CREATE TABLE "
                            + OUTPUT_TABLE1
                            + " ("
                            + "cnt BIGINT NOT NULL DEFAULT 0,"
                            + "lencnt BIGINT NOT NULL DEFAULT 0,"
                            + "cTag INT NOT NULL DEFAULT 0,"
                            + "ts TIMESTAMP,"
                            + "PRIMARY KEY (cnt, cTag))");

            stat.executeUpdate(
                    "CREATE TABLE "
                            + OUTPUT_TABLE2
                            + " ("
                            + "id INT NOT NULL DEFAULT 0,"
                            + "num BIGINT NOT NULL DEFAULT 0,"
                            + "ts TIMESTAMP)");

            stat.executeUpdate(
                    "CREATE TABLE "
                            + OUTPUT_TABLE3
                            + " ("
                            + "NAME VARCHAR(20) NOT NULL,"
                            + "SCORE BIGINT NOT NULL DEFAULT 0)");

            stat.executeUpdate("CREATE TABLE " + OUTPUT_TABLE4 + " (real_data REAL)");

            stat.executeUpdate(
                    "CREATE TABLE " + OUTPUT_TABLE5 + " (" + "id BIGINT NOT NULL DEFAULT 0)");

            stat.executeUpdate(
                    "CREATE TABLE "
                            + USER_TABLE
                            + " ("
                            + "user_id VARCHAR(20) NOT NULL,"
                            + "user_name VARCHAR(20) NOT NULL,"
                            + "email VARCHAR(255),"
                            + "balance DECIMAL(18,2),"
                            + "balance2 DECIMAL(18,2),"
                            + "PRIMARY KEY (user_id))");
        }
    }

    @After
    public void clearOutputTable() throws Exception {
        TestValuesTableFactory.clearAllData();
        Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
        try (Connection conn = DriverManager.getConnection(DB_URL);
                Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + OUTPUT_TABLE1);
            stat.execute("DROP TABLE " + OUTPUT_TABLE2);
            stat.execute("DROP TABLE " + OUTPUT_TABLE3);
            stat.execute("DROP TABLE " + OUTPUT_TABLE4);
            stat.execute("DROP TABLE " + OUTPUT_TABLE5);
            stat.execute("DROP TABLE " + USER_TABLE);
        }
    }

    public static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(
            StreamExecutionEnvironment env) {
        List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
        data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
        data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
        data.add(
                new Tuple4<>(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
        data.add(
                new Tuple4<>(
                        6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
        data.add(new Tuple4<>(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
        data.add(new Tuple4<>(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));
        data.add(new Tuple4<>(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")));
        data.add(new Tuple4<>(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")));
        data.add(new Tuple4<>(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")));
        data.add(new Tuple4<>(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")));
        data.add(new Tuple4<>(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")));
        data.add(new Tuple4<>(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")));
        data.add(new Tuple4<>(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")));
        data.add(new Tuple4<>(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")));
        data.add(new Tuple4<>(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")));
        data.add(new Tuple4<>(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")));
        data.add(new Tuple4<>(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")));
        data.add(new Tuple4<>(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")));
        data.add(new Tuple4<>(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));

        Collections.shuffle(data);
        return env.fromCollection(data);
    }

    @Test
    public void testReal() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  real_data float"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE4
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO upsertSink SELECT CAST(1.0 as FLOAT)").await();
        check(new Row[] {Row.of(1.0f)}, DB_URL, "REAL_TABLE", new String[] {"real_data"});
    }

    @Test
    public void testUpsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        Table t =
                tEnv.fromDataStream(
                        get4TupleDataStream(env)
                                .assignTimestampsAndWatermarks(
                                        new AscendingTimestampExtractor<
                                                Tuple4<Integer, Long, String, Timestamp>>() {
                                            @Override
                                            public long extractAscendingTimestamp(
                                                    Tuple4<Integer, Long, String, Timestamp>
                                                            element) {
                                                return element.f0;
                                            }
                                        }),
                        $("id"),
                        $("num"),
                        $("text"),
                        $("ts"));

        tEnv.createTemporaryView("T", t);
        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  cnt BIGINT,"
                        + "  lencnt BIGINT,"
                        + "  cTag INT,"
                        + "  ts TIMESTAMP(3),"
                        + "  PRIMARY KEY (cnt, cTag) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE1
                        + "',"
                        + "  'sink.buffer-flush.max-rows' = '2',"
                        + "  'sink.buffer-flush.interval' = '0',"
                        + "  'sink.max-retries' = '0'"
                        + ")");

        tEnv.executeSql(
                        "INSERT INTO upsertSink \n"
                                + "SELECT cnt, COUNT(len) AS lencnt, cTag, MAX(ts) AS ts\n"
                                + "FROM (\n"
                                + "  SELECT len, COUNT(id) as cnt, cTag, MAX(ts) AS ts\n"
                                + "  FROM (SELECT id, CHAR_LENGTH(text) AS len, (CASE WHEN id > 0 THEN 1 ELSE 0 END) cTag, ts FROM T)\n"
                                + "  GROUP BY len, cTag\n"
                                + ")\n"
                                + "GROUP BY cnt, cTag")
                .await();
        check(
                new Row[] {
                    Row.of(1, 5, 1, Timestamp.valueOf("1970-01-01 00:00:00.006")),
                    Row.of(7, 1, 1, Timestamp.valueOf("1970-01-01 00:00:00.021")),
                    Row.of(9, 1, 1, Timestamp.valueOf("1970-01-01 00:00:00.015"))
                },
                DB_URL,
                OUTPUT_TABLE1,
                new String[] {"cnt", "lencnt", "cTag", "ts"});
    }

    @Test
    public void testAppend() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table t =
                tEnv.fromDataStream(
                        get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

        tEnv.registerTable("T", t);

        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  num BIGINT,"
                        + "  ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector'='jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name'='"
                        + OUTPUT_TABLE2
                        + "'"
                        + ")");

        tEnv.executeSql("INSERT INTO upsertSink SELECT id, num, ts FROM T WHERE id IN (2, 10, 20)")
                .await();
        check(
                new Row[] {
                    Row.of(2, 2, Timestamp.valueOf("1970-01-01 00:00:00.002")),
                    Row.of(10, 4, Timestamp.valueOf("1970-01-01 00:00:00.01")),
                    Row.of(20, 6, Timestamp.valueOf("1970-01-01 00:00:00.02"))
                },
                DB_URL,
                OUTPUT_TABLE2,
                new String[] {"id", "num", "ts"});
    }

    @Test
    public void testBatchSink() throws Exception {
        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);

        tEnv.executeSql(
                "CREATE TABLE USER_RESULT("
                        + "NAME VARCHAR,"
                        + "SCORE BIGINT"
                        + ") WITH ( "
                        + "'connector' = 'jdbc',"
                        + "'url'='"
                        + DB_URL
                        + "',"
                        + "'table-name' = '"
                        + OUTPUT_TABLE3
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '2',"
                        + "'sink.buffer-flush.interval' = '300ms',"
                        + "'sink.max-retries' = '4'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();

        check(
                new Row[] {
                    Row.of("Bob", 1),
                    Row.of("Tom", 22),
                    Row.of("Kim", 42),
                    Row.of("Kim", 42),
                    Row.of("Bob", 1)
                },
                DB_URL,
                OUTPUT_TABLE3,
                new String[] {"NAME", "SCORE"});
    }

    @Test
    public void testReadingFromChangelogSource() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        String dataId = TestValuesTableFactory.registerData(TestData.userChangelog());
        tEnv.executeSql(
                "CREATE TABLE user_logs (\n"
                        + "  user_id STRING,\n"
                        + "  user_name STRING,\n"
                        + "  email STRING,\n"
                        + "  balance DECIMAL(18,2),\n"
                        + "  balance2 AS balance * 2\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'data-id' = '"
                        + dataId
                        + "',\n"
                        + " 'changelog-mode' = 'I,UA,UB,D'\n"
                        + ")");
        tEnv.executeSql(
                "CREATE TABLE user_sink (\n"
                        + "  user_id STRING PRIMARY KEY NOT ENFORCED,\n"
                        + "  user_name STRING,\n"
                        + "  email STRING,\n"
                        + "  balance DECIMAL(18,2),\n"
                        + "  balance2 DECIMAL(18,2)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'jdbc',"
                        + "  'url'='"
                        + DB_URL
                        + "',"
                        + "  'table-name' = '"
                        + USER_TABLE
                        + "',"
                        + "  'sink.buffer-flush.max-rows' = '2',"
                        + "  'sink.buffer-flush.interval' = '0'"
                        + // disable async flush
                        ")");
        tEnv.executeSql("INSERT INTO user_sink SELECT * FROM user_logs").await();

        check(
                new Row[] {
                    Row.of(
                            "user1",
                            "Tom",
                            "tom123@gmail.com",
                            new BigDecimal("8.10"),
                            new BigDecimal("16.20")),
                    Row.of(
                            "user3",
                            "Bailey",
                            "bailey@qq.com",
                            new BigDecimal("9.99"),
                            new BigDecimal("19.98")),
                    Row.of(
                            "user4",
                            "Tina",
                            "tina@gmail.com",
                            new BigDecimal("11.30"),
                            new BigDecimal("22.60"))
                },
                DB_URL,
                USER_TABLE,
                new String[] {"user_id", "user_name", "email", "balance", "balance2"});
    }

    @Test
    public void testFlushBufferWhenCheckpoint() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", DB_URL);
        options.put("table-name", OUTPUT_TABLE5);
        options.put("sink.buffer-flush.interval", "0");

        TableSchema sinkTableSchema =
                TableSchema.builder().field("id", DataTypes.BIGINT().notNull()).build();

        DynamicTableSink tableSink =
                FactoryUtil.createTableSink(
                        null,
                        ObjectIdentifier.of("default", "default", "checkpoint_sink"),
                        new CatalogTableImpl(sinkTableSchema, options, "mock sink"),
                        new Configuration(),
                        this.getClass().getClassLoader(),
                        false);

        SinkRuntimeProviderContext context = new SinkRuntimeProviderContext(false);
        SinkFunctionProvider sinkProvider =
                (SinkFunctionProvider) tableSink.getSinkRuntimeProvider(context);
        GenericJdbcSinkFunction<RowData> sinkFunction =
                (GenericJdbcSinkFunction<RowData>) sinkProvider.createSinkFunction();
        sinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(true, 1, 0));
        sinkFunction.open(new Configuration());
        sinkFunction.invoke(GenericRowData.of(1L), SinkContextUtil.forTimestamp(1));
        sinkFunction.invoke(GenericRowData.of(2L), SinkContextUtil.forTimestamp(1));

        check(new Row[] {}, DB_URL, OUTPUT_TABLE5, new String[] {"id"});
        sinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        check(new Row[] {Row.of(1L), Row.of(2L)}, DB_URL, OUTPUT_TABLE5, new String[] {"id"});
        sinkFunction.close();
    }
}
