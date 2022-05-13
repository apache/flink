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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.types.Row;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.typeinfo.Types.INT;
import static org.apache.flink.api.common.typeinfo.Types.LOCAL_DATE_TIME;
import static org.apache.flink.api.common.typeinfo.Types.ROW_NAMED;
import static org.apache.flink.api.common.typeinfo.Types.STRING;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectRows;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.comparedWithKeyAndOrder;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.waitingExpectedResults;
import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.utils.TableTestMatchers.deepEqualTo;
import static org.junit.Assert.assertThat;

/** Upsert-kafka IT cases. */
@RunWith(Parameterized.class)
public class UpsertKafkaTableITCase extends KafkaTableTestBase {

    private static final String JSON_FORMAT = "json";
    private static final String CSV_FORMAT = "csv";
    private static final String AVRO_FORMAT = "avro";

    @Parameterized.Parameter public String format;

    @Parameterized.Parameters(name = "format = {0}")
    public static Object[] parameters() {
        return new Object[] {JSON_FORMAT, CSV_FORMAT, AVRO_FORMAT};
    }

    @Rule public final LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    private static final String USERS_TOPIC = "users";
    private static final String WORD_COUNT_TOPIC = "word_count";

    @Test
    public void testAggregate() throws Exception {
        String topic = WORD_COUNT_TOPIC + "_" + format;
        createTestTopic(topic, 4, 1);
        // -------------   test   ---------------
        wordCountToUpsertKafka(topic);
        wordFreqToUpsertKafka(topic);
        // ------------- clean up ---------------
        deleteTestTopic(topic);
    }

    @Test
    public void testTemporalJoin() throws Exception {
        String topic = USERS_TOPIC + "_" + format;
        createTestTopic(topic, 2, 1);
        // -------------   test   ---------------
        // Kafka DefaultPartitioner's hash strategy is slightly different from Flink
        // KeyGroupStreamPartitioner,
        // which causes the records in the different Flink partitions are written into the same
        // Kafka partition.
        // When reading from the out-of-order Kafka partition, we need to set suitable watermark
        // interval to
        // tolerate the disorderliness.
        // For convenience, we just set the parallelism 1 to make all records are in the same Flink
        // partition and
        // use the Kafka DefaultPartition to repartition the records.
        env.setParallelism(1);
        writeChangelogToUpsertKafkaWithMetadata(topic);
        env.setParallelism(2);
        temporalJoinUpsertKafka(topic);
        // ------------- clean up ---------------
        deleteTestTopic(topic);
    }

    @Test
    public void testBufferedUpsertSink() throws Exception {
        final String topic = "buffered_upsert_topic_" + format;
        createTestTopic(topic, 1, 1);
        String bootstraps = getBootstrapServers();
        env.setParallelism(1);

        Table table =
                tEnv.fromDataStream(
                        env.fromElements(
                                        Row.of(
                                                1,
                                                LocalDateTime.parse("2020-03-08T13:12:11.12"),
                                                "payload 1"),
                                        Row.of(
                                                2,
                                                LocalDateTime.parse("2020-03-09T13:12:11.12"),
                                                "payload 2"),
                                        Row.of(
                                                3,
                                                LocalDateTime.parse("2020-03-10T13:12:11.12"),
                                                "payload 3"),
                                        Row.of(
                                                3,
                                                LocalDateTime.parse("2020-03-11T13:12:11.12"),
                                                "payload"))
                                .returns(
                                        ROW_NAMED(
                                                new String[] {"k_id", "ts", "payload"},
                                                INT,
                                                LOCAL_DATE_TIME,
                                                STRING)),
                        Schema.newBuilder()
                                .column("k_id", DataTypes.INT())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .column("payload", DataTypes.STRING())
                                .watermark("ts", "ts")
                                .build());

        final String createTable =
                String.format(
                        "CREATE TABLE upsert_kafka (\n"
                                + "  `k_id` INTEGER,\n"
                                + "  `ts` TIMESTAMP(3),\n"
                                + "  `payload` STRING,\n"
                                + "  PRIMARY KEY (k_id) NOT ENFORCED"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields-prefix' = 'k_',\n"
                                + "  'sink.buffer-flush.max-rows' = '2',\n"
                                + "  'sink.buffer-flush.interval' = '100000',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic, bootstraps, format, format);

        tEnv.executeSql(createTable);

        table.executeInsert("upsert_kafka").await();

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM upsert_kafka"), 3);
        final List<Row> expected =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                1,
                                LocalDateTime.parse("2020-03-08T13:12:11.120"),
                                "payload 1"),
                        changelogRow(
                                "+I",
                                2,
                                LocalDateTime.parse("2020-03-09T13:12:11.120"),
                                "payload 2"),
                        changelogRow(
                                "+I",
                                3,
                                LocalDateTime.parse("2020-03-11T13:12:11.120"),
                                "payload"));

        assertThat(result, deepEqualTo(expected, true));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testSourceSinkWithKeyAndPartialValue() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_partial_value_topic_" + format;
        createTestTopic(topic, 1, 1); // use single partition to guarantee orders in tests

        // ---------- Produce an event time stream into Kafka -------------------
        String bootstraps = getBootstrapServers();

        // k_user_id and user_id have different data types to verify the correct mapping,
        // fields are reordered on purpose
        final String createTable =
                String.format(
                        "CREATE TABLE upsert_kafka (\n"
                                + "  `k_user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `k_event_id` BIGINT,\n"
                                + "  `user_id` INT,\n"
                                + "  `payload` STRING,\n"
                                + "  PRIMARY KEY (k_event_id, k_user_id) NOT ENFORCED"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields-prefix' = 'k_',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'\n"
                                + ")",
                        topic, bootstraps, format, format);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO upsert_kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 41, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 42, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 43, 'payload 3'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-11 13:12:11.123', 101, 42, 'payload')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM upsert_kafka"), 5);

        final List<Row> expected =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                41,
                                "payload 1"),
                        changelogRow(
                                "+I",
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                42,
                                "payload 2"),
                        changelogRow(
                                "+I",
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                43,
                                "payload 3"),
                        changelogRow(
                                "-U",
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                42,
                                "payload 2"),
                        changelogRow(
                                "+U",
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-11T13:12:11.123"),
                                101L,
                                42,
                                "payload"));

        assertThat(result, deepEqualTo(expected, true));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    @Test
    public void testKafkaSourceSinkWithKeyAndFullValue() throws Exception {
        // we always use a different topic name for each parameterized topic,
        // in order to make sure the topic can be created.
        final String topic = "key_full_value_topic_" + format;
        createTestTopic(topic, 1, 1); // use single partition to guarantee orders in tests

        // ---------- Produce an event time stream into Kafka -------------------
        String bootstraps = getBootstrapServers();

        // compared to the partial value test we cannot support both k_user_id and user_id in a full
        // value due to duplicate names after key prefix stripping,
        // fields are reordered on purpose,
        // fields for keys and values are overlapping
        final String createTable =
                String.format(
                        "CREATE TABLE upsert_kafka (\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `name` STRING,\n"
                                + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                                + "  `event_id` BIGINT,\n"
                                + "  `payload` STRING,\n"
                                + "  PRIMARY KEY (event_id, user_id) NOT ENFORCED"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'ALL',\n"
                                + "  'sink.parallelism' = '4'" // enable different parallelism to
                                // check ordering
                                + ")",
                        topic, bootstraps, format, format);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO upsert_kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', 100, 'payload 1'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-09 13:12:11.123', 101, 'payload 2'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-10 13:12:11.123', 102, 'payload 3'),\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-11 13:12:11.123', 100, 'payload')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM upsert_kafka"), 5);

        final List<Row> expected =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                "payload 1"),
                        changelogRow(
                                "+I",
                                2L,
                                "name 2",
                                LocalDateTime.parse("2020-03-09T13:12:11.123"),
                                101L,
                                "payload 2"),
                        changelogRow(
                                "+I",
                                3L,
                                "name 3",
                                LocalDateTime.parse("2020-03-10T13:12:11.123"),
                                102L,
                                "payload 3"),
                        changelogRow(
                                "-U",
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-08T13:12:11.123"),
                                100L,
                                "payload 1"),
                        changelogRow(
                                "+U",
                                1L,
                                "name 1",
                                LocalDateTime.parse("2020-03-11T13:12:11.123"),
                                100L,
                                "payload"));

        assertThat(result, deepEqualTo(expected, true));

        // ------------- cleanup -------------------

        deleteTestTopic(topic);
    }

    private void wordCountToUpsertKafka(String wordCountTable) throws Exception {
        String bootstraps = getBootstrapServers();

        // ------------- test data ---------------

        final List<Row> inputData =
                Arrays.stream("Good good study day day up Good boy".split(" "))
                        .map(Row::of)
                        .collect(Collectors.toList());

        // ------------- create table ---------------

        final String createSource =
                String.format(
                        "CREATE TABLE words_%s ("
                                + "  `word` STRING"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s'"
                                + ")",
                        format, TestValuesTableFactory.registerData(inputData));
        tEnv.executeSql(createSource);
        final String createSinkTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `word` STRING,\n"
                                + "  `count` BIGINT,\n"
                                + "  PRIMARY KEY (`word`) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'value.format' = '%s'"
                                + ")",
                        wordCountTable, wordCountTable, bootstraps, format, format);
        tEnv.executeSql(createSinkTable);
        String initialValues =
                "INSERT INTO "
                        + wordCountTable
                        + " "
                        + "SELECT LOWER(`word`), count(*) as `count` "
                        + "FROM words_"
                        + format
                        + " "
                        + "GROUP BY LOWER(`word`)";
        tEnv.executeSql(initialValues).await();

        // ---------- read from the upsert sink -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM " + wordCountTable), 11);

        final Map<Row, List<Row>> expected = new HashMap<>();
        expected.put(
                Row.of("good"),
                Arrays.asList(
                        changelogRow("+I", "good", 1L),
                        changelogRow("-U", "good", 1L),
                        changelogRow("+U", "good", 2L),
                        changelogRow("-U", "good", 2L),
                        changelogRow("+U", "good", 3L)));
        expected.put(Row.of("study"), Collections.singletonList(changelogRow("+I", "study", 1L)));
        expected.put(
                Row.of("day"),
                Arrays.asList(
                        changelogRow("+I", "day", 1L),
                        changelogRow("-U", "day", 1L),
                        changelogRow("+U", "day", 2L)));
        expected.put(Row.of("up"), Collections.singletonList(changelogRow("+I", "up", 1L)));
        expected.put(Row.of("boy"), Collections.singletonList(changelogRow("+I", "boy", 1L)));

        comparedWithKeyAndOrder(expected, result, new int[] {0});

        // ---------- read the raw data from kafka -------------------
        // check we only write the upsert data into Kafka
        String rawWordCountTable = "raw_word_count";
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `key_word` STRING NOT NULL,\n"
                                + "  `word` STRING NOT NULL,\n"
                                + "  `count` BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'key.fields' = 'key_word',\n"
                                + "  'key.fields-prefix' = 'key_',\n"
                                + "  'value.format' = '%s',\n"
                                + "  'value.fields-include' = 'EXCEPT_KEY'"
                                + ")",
                        rawWordCountTable, wordCountTable, bootstraps, format, format));

        final List<Row> result2 =
                collectRows(tEnv.sqlQuery("SELECT * FROM " + rawWordCountTable), 8);
        final Map<Row, List<Row>> expected2 = new HashMap<>();
        expected2.put(
                Row.of("good"),
                Arrays.asList(
                        Row.of("good", "good", 1L),
                        Row.of("good", "good", 2L),
                        Row.of("good", "good", 3L)));
        expected2.put(Row.of("study"), Collections.singletonList(Row.of("study", "study", 1L)));
        expected2.put(
                Row.of("day"), Arrays.asList(Row.of("day", "day", 1L), Row.of("day", "day", 2L)));
        expected2.put(Row.of("up"), Collections.singletonList(Row.of("up", "up", 1L)));
        expected2.put(Row.of("boy"), Collections.singletonList(Row.of("boy", "boy", 1L)));

        comparedWithKeyAndOrder(expected2, result2, new int[] {0});
    }

    private void wordFreqToUpsertKafka(String wordCountTable) throws Exception {
        // ------------- test data ---------------

        final List<String> expectedData = Arrays.asList("3,1", "2,1");

        // ------------- create table ---------------

        final String createSinkTable =
                "CREATE TABLE sink_"
                        + format
                        + "(\n"
                        + "  `count` BIGINT,\n"
                        + "  `freq` BIGINT,\n"
                        + "  PRIMARY KEY (`count`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false'\n"
                        + ")";
        tEnv.executeSql(createSinkTable);
        final TableResult query =
                tEnv.executeSql(
                        "INSERT INTO sink_"
                                + format
                                + "\n"
                                + "SELECT `count`, count(*) as `freq`\n"
                                + "FROM "
                                + wordCountTable
                                + "\n"
                                + "GROUP BY `count`\n"
                                + "having count(*) < 2");

        // ---------- consume stream from sink -------------------

        waitingExpectedResults("sink_" + format, expectedData, Duration.ofSeconds(10));
        query.getJobClient().get().cancel();
    }

    private void writeChangelogToUpsertKafkaWithMetadata(String userTable) throws Exception {
        String bootstraps = getBootstrapServers();

        // ------------- test data ---------------

        // Prepare data for upsert kafka
        // Keep every partition has more than 1 record and the last record in every partition has
        // larger event time
        // than left stream event time to trigger the join.
        List<Row> changelogData =
                Arrays.asList(
                        changelogRow(
                                "+U",
                                100L,
                                "Bob",
                                "Beijing",
                                LocalDateTime.parse("2020-08-15T00:00:01")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Shanghai",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        changelogRow(
                                "+U",
                                102L,
                                "Greg",
                                "Berlin",
                                LocalDateTime.parse("2020-08-15T00:00:03")),
                        changelogRow(
                                "+U",
                                103L,
                                "Richard",
                                "Berlin",
                                LocalDateTime.parse("2020-08-16T00:01:05")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Wuhan",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        changelogRow(
                                "+U",
                                104L,
                                "Tomato",
                                "Hongkong",
                                LocalDateTime.parse("2020-08-16T00:05:05")),
                        changelogRow(
                                "+U",
                                105L,
                                "Tim",
                                "Shenzhen",
                                LocalDateTime.parse("2020-08-16T00:06:00")),
                        // Keep the timestamp in the records are in the ascending order.
                        // It will keep the records in the kafka partition are in the order.
                        // It has the same effects by adjusting the watermark strategy.
                        changelogRow(
                                "+U",
                                103L,
                                "Richard",
                                "London",
                                LocalDateTime.parse("2020-08-16T01:01:05")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Hangzhou",
                                LocalDateTime.parse("2020-08-16T01:04:05")),
                        changelogRow(
                                "+U",
                                104L,
                                "Tomato",
                                "Shenzhen",
                                LocalDateTime.parse("2020-08-16T01:05:05")),
                        changelogRow(
                                "+U",
                                105L,
                                "Tim",
                                "Hongkong",
                                LocalDateTime.parse("2020-08-16T01:06:00")));

        // ------------- create table ---------------

        final String createChangelog =
                String.format(
                        "CREATE TABLE users_changelog_%s ("
                                + "  user_id BIGINT,"
                                + "  user_name STRING,"
                                + "  region STRING,"
                                + "  modification_time TIMESTAMP(3),"
                                + "  PRIMARY KEY (user_id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'data-id' = '%s',"
                                + "  'changelog-mode' = 'UA,D',"
                                + "  'disable-lookup' = 'true'"
                                + ")",
                        format, TestValuesTableFactory.registerData(changelogData));
        tEnv.executeSql(createChangelog);

        // we verified computed column, watermark, metadata in this case too
        final String createSinkTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `user_name` STRING,\n"
                                + "  `region` STRING,\n"
                                + "  upper_region AS UPPER(`region`),\n"
                                + "  modification_time TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                                + "  watermark for modification_time as modification_time,\n"
                                + "  PRIMARY KEY (`user_id`) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'upsert-kafka',\n"
                                + "  'topic' = '%s',\n"
                                + "  'properties.bootstrap.servers' = '%s',\n"
                                + "  'key.format' = '%s',\n"
                                + "  'value.format' = '%s'"
                                + ")",
                        userTable, userTable, bootstraps, format, format);
        tEnv.executeSql(createSinkTable);
        String initialValues =
                "INSERT INTO " + userTable + " " + "SELECT * " + "FROM users_changelog_" + format;
        tEnv.executeSql(initialValues).await();

        // ---------- consume stream from sink -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM " + userTable), 16);

        List<Row> expected =
                Arrays.asList(
                        changelogRow(
                                "+I",
                                100L,
                                "Bob",
                                "Beijing",
                                "BEIJING",
                                LocalDateTime.parse("2020-08-15T00:00:01")),
                        changelogRow(
                                "+I",
                                101L,
                                "Alice",
                                "Shanghai",
                                "SHANGHAI",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        changelogRow(
                                "-U",
                                101L,
                                "Alice",
                                "Shanghai",
                                "SHANGHAI",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Wuhan",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        changelogRow(
                                "-U",
                                101L,
                                "Alice",
                                "Wuhan",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        changelogRow(
                                "+U",
                                101L,
                                "Alice",
                                "Hangzhou",
                                "HANGZHOU",
                                LocalDateTime.parse("2020-08-16T01:04:05")),
                        changelogRow(
                                "+I",
                                102L,
                                "Greg",
                                "Berlin",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-15T00:00:03")),
                        changelogRow(
                                "+I",
                                103L,
                                "Richard",
                                "Berlin",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-16T00:01:05")),
                        changelogRow(
                                "-U",
                                103L,
                                "Richard",
                                "Berlin",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-16T00:01:05")),
                        changelogRow(
                                "+U",
                                103L,
                                "Richard",
                                "London",
                                "LONDON",
                                LocalDateTime.parse("2020-08-16T01:01:05")),
                        changelogRow(
                                "+I",
                                104L,
                                "Tomato",
                                "Hongkong",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T00:05:05")),
                        changelogRow(
                                "-U",
                                104L,
                                "Tomato",
                                "Hongkong",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T00:05:05")),
                        changelogRow(
                                "+U",
                                104L,
                                "Tomato",
                                "Shenzhen",
                                "SHENZHEN",
                                LocalDateTime.parse("2020-08-16T01:05:05")),
                        changelogRow(
                                "+I",
                                105L,
                                "Tim",
                                "Shenzhen",
                                "SHENZHEN",
                                LocalDateTime.parse("2020-08-16T00:06")),
                        changelogRow(
                                "-U",
                                105L,
                                "Tim",
                                "Shenzhen",
                                "SHENZHEN",
                                LocalDateTime.parse("2020-08-16T00:06")),
                        changelogRow(
                                "+U",
                                105L,
                                "Tim",
                                "Hongkong",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T01:06")));

        // we ignore the orders for easier comparing, as we already verified ordering in
        // testAggregate()
        assertThat(result, deepEqualTo(expected, true));
    }

    private void temporalJoinUpsertKafka(String userTable) throws Exception {
        // ------------- test data ---------------
        List<Row> input =
                Arrays.asList(
                        Row.of(10001L, 100L, LocalDateTime.parse("2020-08-15T00:00:02")),
                        Row.of(10002L, 101L, LocalDateTime.parse("2020-08-15T00:00:03")),
                        Row.of(10002L, 102L, LocalDateTime.parse("2020-08-15T00:00:04")),
                        Row.of(10002L, 101L, LocalDateTime.parse("2020-08-16T00:02:01")),
                        Row.of(10004L, 104L, LocalDateTime.parse("2020-08-16T00:04:00")),
                        Row.of(10003L, 101L, LocalDateTime.parse("2020-08-16T00:04:06")),
                        Row.of(10004L, 104L, LocalDateTime.parse("2020-08-16T00:05:06")));

        List<Row> expected =
                Arrays.asList(
                        Row.of(
                                10001L,
                                100L,
                                LocalDateTime.parse("2020-08-15T00:00:02"),
                                "Bob",
                                "BEIJING",
                                LocalDateTime.parse("2020-08-15T00:00:01")),
                        Row.of(
                                10002L,
                                101L,
                                LocalDateTime.parse("2020-08-15T00:00:03"),
                                "Alice",
                                "SHANGHAI",
                                LocalDateTime.parse("2020-08-15T00:00:02")),
                        Row.of(
                                10002L,
                                102L,
                                LocalDateTime.parse("2020-08-15T00:00:04"),
                                "Greg",
                                "BERLIN",
                                LocalDateTime.parse("2020-08-15T00:00:03")),
                        Row.of(
                                10002L,
                                101L,
                                LocalDateTime.parse("2020-08-16T00:02:01"),
                                "Alice",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        Row.of(
                                10004L,
                                104L,
                                LocalDateTime.parse("2020-08-16T00:04:00"),
                                null,
                                null,
                                null),
                        Row.of(
                                10003L,
                                101L,
                                LocalDateTime.parse("2020-08-16T00:04:06"),
                                "Alice",
                                "WUHAN",
                                LocalDateTime.parse("2020-08-16T00:02:00")),
                        Row.of(
                                10004L,
                                104L,
                                LocalDateTime.parse("2020-08-16T00:05:06"),
                                "Tomato",
                                "HONGKONG",
                                LocalDateTime.parse("2020-08-16T00:05:05")));

        // ------------- create table ---------------

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE pageviews_%s(\n"
                                + "  `page_id` BIGINT,\n"
                                + "  `user_id` BIGINT,\n"
                                + "  `viewtime` TIMESTAMP(3),\n"
                                + "  `proctime` as proctime(),\n"
                                + "   watermark for `viewtime` as `viewtime`\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s'\n"
                                + ")",
                        format, TestValuesTableFactory.registerData(input)));

        final List<Row> result =
                collectRows(
                        tEnv.sqlQuery(
                                String.format(
                                        "SELECT p.page_id, p.user_id, p.viewtime, u.user_name, u.upper_region, u.modification_time\n"
                                                + "FROM pageviews_%s AS p\n"
                                                + "LEFT JOIN %s FOR SYSTEM_TIME AS OF p.viewtime AS u\n"
                                                + "ON p.user_id = u.user_id",
                                        format, userTable)),
                        7);

        assertThat(result, deepEqualTo(expected, true));
    }
}
