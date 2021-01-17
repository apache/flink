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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBaseWithFlink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectRows;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.readLines;
import static org.apache.flink.table.utils.TableTestMatchers.deepEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Basic IT cases for the Kafka table source and sink. */
@RunWith(Parameterized.class)
public class KafkaTableITCase extends KafkaTestBaseWithFlink {

    private static final String JSON_FORMAT = "json";
    private static final String AVRO_FORMAT = "avro";
    private static final String CSV_FORMAT = "csv";

    @Parameterized.Parameter public boolean isLegacyConnector;

    @Parameterized.Parameter(1)
    public String format;

    @Parameterized.Parameters(name = "legacy = {0}, format = {1}")
    public static Object[] parameters() {
        return new Object[][] {
            // cover all 3 formats for new and old connector
            new Object[] {false, JSON_FORMAT},
            new Object[] {false, AVRO_FORMAT},
            new Object[] {false, CSV_FORMAT},
            new Object[] {true, JSON_FORMAT},
            new Object[] {true, AVRO_FORMAT},
            new Object[] {true, CSV_FORMAT}
        };
    }

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv =
                StreamTableEnvironment.create(
                        env,
                        EnvironmentSettings.newInstance()
                                // Watermark is only supported in blink planner
                                .useBlinkPlanner()
                                .inStreamingMode()
                                .build());
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    @Test
    public void testKafkaSourceSink() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "tstopic_" + format + "_" + isLegacyConnector;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");

        final String createTable;
        if (!isLegacyConnector) {
            createTable =
                    String.format(
                            "create table kafka (\n"
                                    + "  `computed-price` as price + 1.0,\n"
                                    + "  price decimal(38, 18),\n"
                                    + "  currency string,\n"
                                    + "  log_date date,\n"
                                    + "  log_time time(3),\n"
                                    + "  log_ts timestamp(3),\n"
                                    + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                                    + "  watermark for ts as ts\n"
                                    + ") with (\n"
                                    + "  'connector' = '%s',\n"
                                    + "  'topic' = '%s',\n"
                                    + "  'properties.bootstrap.servers' = '%s',\n"
                                    + "  'properties.group.id' = '%s',\n"
                                    + "  'scan.startup.mode' = 'earliest-offset',\n"
                                    + "  %s\n"
                                    + ")",
                            KafkaDynamicTableFactory.IDENTIFIER,
                            topic,
                            bootstraps,
                            groupId,
                            formatOptions());
        } else {
            createTable =
                    String.format(
                            "create table kafka (\n"
                                    + "  `computed-price` as price + 1.0,\n"
                                    + "  price decimal(38, 18),\n"
                                    + "  currency string,\n"
                                    + "  log_date date,\n"
                                    + "  log_time time(3),\n"
                                    + "  log_ts timestamp(3),\n"
                                    + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                                    + "  watermark for ts as ts\n"
                                    + ") with (\n"
                                    + "  'connector.type' = 'kafka',\n"
                                    + "  'connector.version' = '%s',\n"
                                    + "  'connector.topic' = '%s',\n"
                                    + "  'connector.properties.bootstrap.servers' = '%s',\n"
                                    + "  'connector.properties.group.id' = '%s',\n"
                                    + "  'connector.startup-mode' = 'earliest-offset',\n"
                                    + "  'update-mode' = 'append',\n"
                                    + "  %s\n"
                                    + ")",
                            KafkaValidator.CONNECTOR_VERSION_VALUE_UNIVERSAL,
                            topic,
                            bootstraps,
                            groupId,
                            formatOptions());
        }

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "SELECT CAST(price AS DECIMAL(10, 2)), currency, "
                        + " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n"
                        + "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n"
                        + "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n"
                        + "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n"
                        + "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n"
                        + "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n"
                        + "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n"
                        + "  AS orders (price, currency, d, t, ts)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        String query =
                "SELECT\n"
                        + "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n"
                        + "  CAST(MAX(log_date) AS VARCHAR),\n"
                        + "  CAST(MAX(log_time) AS VARCHAR),\n"
                        + "  CAST(MAX(ts) AS VARCHAR),\n"
                        + "  COUNT(*),\n"
                        + "  CAST(MAX(price) AS DECIMAL(10, 2))\n"
                        + "FROM kafka\n"
                        + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("Job_2");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

        List<String> expected =
                Arrays.asList(
                        "+I(2019-12-12 00:00:05.000,2019-12-12,00:00:03,2019-12-12 00:00:04.004,3,50.00)",
                        "+I(2019-12-12 00:00:10.000,2019-12-12,00:00:05,2019-12-12 00:00:06.006,2,5.33)");

        assertEquals(expected, TestingSinkFunction.rows);

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testKafkaTableWithMultipleTopics() throws Exception {
        if (isLegacyConnector) {
            return;
        }
        // ---------- create source and sink tables -------------------
        String tableTemp =
                "create table %s (\n"
                        + "  currency string\n"
                        + ") with (\n"
                        + "  'connector' = '%s',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = '%s',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  %s\n"
                        + ")";
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");
        List<String> currencies = Arrays.asList("Euro", "Dollar", "Yen", "Dummy");
        List<String> topics =
                currencies.stream()
                        .map(currency -> String.format("%s_%s", currency, format))
                        .collect(Collectors.toList());
        // Because kafka connector currently doesn't support write data into multiple topic
        // together,
        // we have to create multiple sink tables.
        IntStream.range(0, 4)
                .forEach(
                        index -> {
                            createTestTopic(topics.get(index), 1, 1);
                            tEnv.executeSql(
                                    String.format(
                                            tableTemp,
                                            currencies.get(index).toLowerCase(),
                                            KafkaDynamicTableFactory.IDENTIFIER,
                                            topics.get(index),
                                            bootstraps,
                                            groupId,
                                            formatOptions()));
                        });
        // create source table
        tEnv.executeSql(
                String.format(
                        tableTemp,
                        "currencies_topic_list",
                        KafkaDynamicTableFactory.IDENTIFIER,
                        String.join(";", topics),
                        bootstraps,
                        groupId,
                        formatOptions()));

        // ---------- Prepare data in Kafka topics -------------------
        String insertTemp =
                "INSERT INTO %s\n"
                        + "SELECT currency\n"
                        + " FROM (VALUES ('%s'))\n"
                        + " AS orders (currency)";
        currencies.forEach(
                currency -> {
                    try {
                        tEnv.executeSql(String.format(insertTemp, currency.toLowerCase(), currency))
                                .await();
                    } catch (Exception e) {
                        fail(e.getMessage());
                    }
                });

        // ------------- test the topic-list kafka source -------------------
        DataStream<RowData> result =
                tEnv.toAppendStream(
                        tEnv.sqlQuery("SELECT currency FROM currencies_topic_list"), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(4); // expect to receive 4 records
        result.addSink(sink);

        try {
            env.execute("Job_3");
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered Kafka source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }
        List<String> expected = Arrays.asList("+I(Dollar)", "+I(Dummy)", "+I(Euro)", "+I(Yen)");
        TestingSinkFunction.rows.sort(Comparator.naturalOrder());
        assertEquals(expected, TestingSinkFunction.rows);

        // ------------- cleanup -------------------
        topics.forEach(KafkaTestBase::deleteTestTopic);
    }

    @Test
    public void testKafkaSourceSinkWithMetadata() throws Exception {
        if (isLegacyConnector) {
            return;
        }
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "metadata_topic_" + format;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `physical_1` STRING,\n"
                                + "  `physical_2` INT,\n"
                                // metadata fields are out of order on purpose
                                // offset is ignored because it might not be deterministic
                                + "  `timestamp-type` STRING METADATA VIRTUAL,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `leader-epoch` INT METADATA VIRTUAL,\n"
                                + "  `headers` MAP<STRING, BYTES> METADATA,\n"
                                + "  `partition` INT METADATA VIRTUAL,\n"
                                + "  `topic` STRING METADATA VIRTUAL,\n"
                                + "  `physical_3` BOOLEAN\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  %s\n"
                                + ")",
                        topic, bootstraps, groupId, formatOptions());

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " ('data 1', 1, TIMESTAMP '2020-03-08 13:12:11.123', MAP['k1', X'C0FFEE', 'k2', X'BABE'], TRUE),\n"
                        + " ('data 2', 2, TIMESTAMP '2020-03-09 13:12:11.123', CAST(NULL AS MAP<STRING, BYTES>), FALSE),\n"
                        + " ('data 3', 3, TIMESTAMP '2020-03-10 13:12:11.123', MAP['k1', X'10', 'k2', X'20'], TRUE)";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final Map<String, byte[]> headers1 = new HashMap<>();
        headers1.put("k1", new byte[] {(byte) 0xC0, (byte) 0xFF, (byte) 0xEE});
        headers1.put("k2", new byte[] {(byte) 0xBA, (byte) 0xBE});

        final Map<String, byte[]> headers3 = new HashMap<>();
        headers3.put("k1", new byte[] {(byte) 0x10});
        headers3.put("k2", new byte[] {(byte) 0x20});

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                "data 1",
                                1,
                                "CreateTime",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                0,
                                headers1,
                                0,
                                topic,
                                true),
                        Row.of(
                                "data 2",
                                2,
                                "CreateTime",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                0,
                                Collections.emptyMap(),
                                0,
                                topic,
                                false),
                        Row.of(
                                "data 3",
                                3,
                                "CreateTime",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                0,
                                headers3,
                                0,
                                topic,
                                true));

        assertThat(result, deepEqualTo(expected, true));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testKafkaSourceSinkWithKeyAndPartialValue() throws Exception {
        if (isLegacyConnector) {
            return;
        }
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_partial_value_topic_" + format;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");

        // k_user_id and user_id have different data types to verify the correct mapping,
        // fields are reordered on purpose
        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `k_user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `k_event_id` BIGINT,\n"
                                + "  `user_id` INT,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields' = 'k_event_id; k_user_id',\n"
                                + "  'key.fields-prefix' = 'k_',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic, bootstraps, groupId, format, format);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 41, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 42, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 43, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                41,
                                "payload 1"),
                        Row.of(
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                42,
                                "payload 2"),
                        Row.of(
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                43,
                                "payload 3"));

        assertThat(result, deepEqualTo(expected, true));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testKafkaSourceSinkWithKeyAndFullValue() throws Exception {
        if (isLegacyConnector) {
            return;
        }
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_full_value_topic_" + format;
        createTestTopic(topic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");

        // compared to the partial value test we cannot support both k_user_id and user_id in a full
        // value due to duplicate names after key prefix stripping,
        // fields are reordered on purpose,
        // fields for keys and values are overlapping
        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `event_id` BIGINT,\n"
                                + "  `payload` STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields' = 'event_id; user_id',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'ALL'\n"
                                + ")",
                        topic, bootstraps, groupId, format, format);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                "payload 1"),
                        Row.of(
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                "payload 2"),
                        Row.of(
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                "payload 3"));

        assertThat(result, deepEqualTo(expected, true));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testKafkaTemporalJoinChangelog() throws Exception {
        if (isLegacyConnector) {
            return;
        }

        // Set the session time zone to UTC, because the next `METADATA FROM
        // 'value.source.timestamp'` DDL
        // will use the session time zone when convert the changelog time from milliseconds to
        // timestamp
        tEnv.getConfig().getConfiguration().setString("table.local-time-zone", "UTC");

        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String orderTopic = "temporal_join_topic_order_" + format;
        createTestTopic(orderTopic, 1, 1);

        final String productTopic = "temporal_join_topic_product_" + format;
        createTestTopic(productTopic, 1, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");

        // create order table and set initial values
        final String orderTableDDL =
                String.format(
                        "CREATE TABLE ordersTable (\n"
                                + "  order_id STRING,\n"
                                + "  product_id STRING,\n"
                                + "  order_time TIMESTAMP(3),\n"
                                + "  quantity INT,\n"
                                + "  purchaser STRING,\n"
                                + "  WATERMARK FOR order_time AS order_time - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'format' = '%s'\n"
                                + ")",
                        orderTopic, bootstraps, groupId, format);
        tEnv.executeSql(orderTableDDL);
        String orderInitialValues =
                "INSERT INTO ordersTable\n"
                        + "VALUES\n"
                        + "('o_001', 'p_001', TIMESTAMP '2020-10-01 00:01:00', 1, 'Alice'),"
                        + "('o_002', 'p_002', TIMESTAMP '2020-10-01 00:02:00', 1, 'Bob'),"
                        + "('o_003', 'p_001', TIMESTAMP '2020-10-01 12:00:00', 2, 'Tom'),"
                        + "('o_004', 'p_002', TIMESTAMP '2020-10-01 12:00:00', 2, 'King'),"
                        + "('o_005', 'p_001', TIMESTAMP '2020-10-01 18:00:00', 10, 'Leonard'),"
                        + "('o_006', 'p_002', TIMESTAMP '2020-10-01 18:00:00', 10, 'Leonard'),"
                        + "('o_007', 'p_002', TIMESTAMP '2020-10-01 18:00:01', 10, 'Robinson')"; // used to advance watermark
        tEnv.executeSql(orderInitialValues).await();

        // create product table and set initial values
        final String productTableDDL =
                String.format(
                        "CREATE TABLE productChangelogTable (\n"
                                + "  product_id STRING,\n"
                                + "  product_name STRING,\n"
                                + "  product_price DECIMAL(10, 4),\n"
                                + "  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,\n"
                                + "  PRIMARY KEY(product_id) NOT ENFORCED,\n"
                                + "  WATERMARK FOR update_time AS update_time - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'value.format' = 'debezium-json'\n"
                                + ")",
                        productTopic, bootstraps, groupId);
        tEnv.executeSql(productTableDDL);

        // use raw format to initial the changelog data
        initialProductChangelog(productTopic, bootstraps);

        // ---------- query temporal join result from Kafka -------------------
        final List<String> result =
                collectRows(
                                tEnv.sqlQuery(
                                        "SELECT"
                                                + "  order_id,"
                                                + "  order_time,"
                                                + "  P.product_id,"
                                                + "  P.update_time as product_update_time,"
                                                + "  product_price,"
                                                + "  purchaser,"
                                                + "  product_name,"
                                                + "  quantity,"
                                                + "  quantity * product_price AS order_amount "
                                                + "FROM ordersTable AS O "
                                                + "LEFT JOIN productChangelogTable FOR SYSTEM_TIME AS OF O.order_time AS P "
                                                + "ON O.product_id = P.product_id"),
                                6)
                        .stream()
                        .map(row -> row.toString())
                        .sorted()
                        .collect(Collectors.toList());

        final List<String> expected =
                Arrays.asList(
                        "+I[o_001, 2020-10-01T00:01, p_001, 1970-01-01T00:00, 11.1100, Alice, scooter, 1, 11.1100]",
                        "+I[o_002, 2020-10-01T00:02, p_002, 1970-01-01T00:00, 23.1100, Bob, basketball, 1, 23.1100]",
                        "+I[o_003, 2020-10-01T12:00, p_001, 2020-10-01T12:00, 12.9900, Tom, scooter, 2, 25.9800]",
                        "+I[o_004, 2020-10-01T12:00, p_002, 2020-10-01T12:00, 19.9900, King, basketball, 2, 39.9800]",
                        "+I[o_005, 2020-10-01T18:00, p_001, 2020-10-01T18:00, 11.9900, Leonard, scooter, 10, 119.9000]",
                        "+I[o_006, 2020-10-01T18:00, null, null, null, Leonard, null, 10, null]");

        assertEquals(expected, result);

        // ------------- cleanup -------------------

        deleteTestTopic(orderTopic);
        deleteTestTopic(productTopic);
    }

    private void initialProductChangelog(String topic, String bootstraps) throws Exception {
        String productChangelogDDL =
                String.format(
                        "CREATE TABLE productChangelog (\n"
                                + "  changelog STRING"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'format' = 'raw'\n"
                                + ")",
                        topic, bootstraps);
        tEnv.executeSql(productChangelogDDL);
        String[] allChangelog = readLines("product_changelog.txt").toArray(new String[0]);

        StringBuilder insertSqlSb = new StringBuilder();
        insertSqlSb.append("INSERT INTO productChangelog VALUES ");
        for (String log : allChangelog) {
            insertSqlSb.append("('" + log + "'),");
        }
        // trim the last comma
        String insertSql = insertSqlSb.substring(0, insertSqlSb.toString().length() - 1);
        tEnv.executeSql(insertSql).await();
    }

    @Test
    public void testPerPartitionWatermarkKafka() throws Exception {
        if (isLegacyConnector) {
            return;
        }
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "per_partition_watermark_topic_" + format;
        createTestTopic(topic, 4, 1);

        // ---------- Produce an event time stream into Kafka -------------------
        String groupId = standardProps.getProperty("group.id");
        String bootstraps = standardProps.getProperty("bootstrap.servers");

        final String createTable =
                String.format(
                        "CREATE TABLE kafka (\n"
                                + "  `partition_id` INT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3),\n"
                                + "  WATERMARK FOR `timestamp` AS `timestamp`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'properties.group.id' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'sink.partitioner' = '%s',\n"
                                + "  'format' = '%s'\n"
                                + ")",
                        topic, bootstraps, groupId, TestPartitioner.class.getName(), format);

        tEnv.executeSql(createTable);

        // make every partition have more than one record
        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (0, 'partition-0-name-0', TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (0, 'partition-0-name-1', TIMESTAMP '2020-03-08 14:12:12.223'),\n"
                        + " (0, 'partition-0-name-2', TIMESTAMP '2020-03-08 15:12:13.323'),\n"
                        + " (1, 'partition-1-name-0', TIMESTAMP '2020-03-09 13:13:11.123'),\n"
                        + " (1, 'partition-1-name-1', TIMESTAMP '2020-03-09 15:13:11.133'),\n"
                        + " (1, 'partition-1-name-2', TIMESTAMP '2020-03-09 16:13:11.143'),\n"
                        + " (2, 'partition-2-name-0', TIMESTAMP '2020-03-10 13:12:14.123'),\n"
                        + " (2, 'partition-2-name-1', TIMESTAMP '2020-03-10 14:12:14.123'),\n"
                        + " (2, 'partition-2-name-2', TIMESTAMP '2020-03-10 14:13:14.123'),\n"
                        + " (2, 'partition-2-name-3', TIMESTAMP '2020-03-10 14:14:14.123'),\n"
                        + " (2, 'partition-2-name-4', TIMESTAMP '2020-03-10 14:15:14.123'),\n"
                        + " (2, 'partition-2-name-5', TIMESTAMP '2020-03-10 14:16:14.123'),\n"
                        + " (3, 'partition-3-name-0', TIMESTAMP '2020-03-11 17:12:11.123'),\n"
                        + " (3, 'partition-3-name-1', TIMESTAMP '2020-03-11 18:12:11.123')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        env.setParallelism(1);
        String createSink =
                "CREATE TABLE MySink(\n"
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  ts TIMESTAMP(3),\n"
                        + "  WATERMARK FOR ts as ts\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink.drop-late-event' = 'true'\n"
                        + ")";
        tEnv.executeSql(createSink);
        TableResult tableResult = tEnv.executeSql("INSERT INTO MySink SELECT * FROM kafka");
        final List<String> expected =
                Arrays.asList(
                        "+I[0, partition-0-name-0, 2020-03-08T13:12:11.123]",
                        "+I[0, partition-0-name-1, 2020-03-08T14:12:12.223]",
                        "+I[0, partition-0-name-2, 2020-03-08T15:12:13.323]",
                        "+I[1, partition-1-name-0, 2020-03-09T13:13:11.123]",
                        "+I[1, partition-1-name-1, 2020-03-09T15:13:11.133]",
                        "+I[1, partition-1-name-2, 2020-03-09T16:13:11.143]",
                        "+I[2, partition-2-name-0, 2020-03-10T13:12:14.123]",
                        "+I[2, partition-2-name-1, 2020-03-10T14:12:14.123]",
                        "+I[2, partition-2-name-2, 2020-03-10T14:13:14.123]",
                        "+I[2, partition-2-name-3, 2020-03-10T14:14:14.123]",
                        "+I[2, partition-2-name-4, 2020-03-10T14:15:14.123]",
                        "+I[2, partition-2-name-5, 2020-03-10T14:16:14.123]",
                        "+I[3, partition-3-name-0, 2020-03-11T17:12:11.123]",
                        "+I[3, partition-3-name-1, 2020-03-11T18:12:11.123]");
        KafkaTableTestUtils.waitingExpectedResults("MySink", expected, Duration.ofSeconds(5));

        // ------------- cleanup -------------------

        tableResult.getJobClient().ifPresent(JobClient::cancel);
        deleteTestTopic(topic);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /** Extract the partition id from the row and set it on the record. */
    public static class TestPartitioner extends FlinkKafkaPartitioner<RowData> {

        private static final long serialVersionUID = 1L;
        private static final int PARTITION_ID_FIELD_IN_SCHEMA = 0;

        @Override
        public int partition(
                RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return partitions[record.getInt(PARTITION_ID_FIELD_IN_SCHEMA) % partitions.length];
        }
    }

    private String formatOptions() {
        if (!isLegacyConnector) {
            return String.format("'format' = '%s'", format);
        } else {
            String formatType = String.format("'format.type' = '%s'", format);
            if (format.equals(AVRO_FORMAT)) {
                // legacy connector requires to specify avro-schema
                String avroSchema =
                        "{\"type\":\"record\",\"name\":\"row_0\",\"fields\":"
                                + "[{\"name\":\"price\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
                                + "\"precision\":38,\"scale\":18}},{\"name\":\"currency\",\"type\":[\"string\","
                                + "\"null\"]},{\"name\":\"log_date\",\"type\":{\"type\":\"int\",\"logicalType\":"
                                + "\"date\"}},{\"name\":\"log_time\",\"type\":{\"type\":\"int\",\"logicalType\":"
                                + "\"time-millis\"}},{\"name\":\"log_ts\",\"type\":{\"type\":\"long\","
                                + "\"logicalType\":\"timestamp-millis\"}}]}";
                return formatType + String.format(", 'format.avro-schema' = '%s'", avroSchema);
            } else {
                return formatType;
            }
        }
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value.toString());
            if (rows.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }
}
