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

import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.connector.upserttest.sink.UpsertTestFileUtil;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.DockerImageVersions;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E Test for SqlClient. */
@Testcontainers
public class SqlClientITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClientITCase.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";

    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    private static final Path sqlToolBoxJar = ResourceTestUtils.getResource(".*/SqlToolbox\\.jar");

    private final Path sqlConnectorKafkaJar = ResourceTestUtils.getResource(".*kafka.*\\.jar");

    private final Path sqlConnectorUpsertTestJar =
            ResourceTestUtils.getResource(".*flink-test-utils.*\\.jar");

    public static final Network NETWORK = Network.newNetwork();

    @Container
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withLogConsumer(LOG_CONSUMER);

    public final FlinkContainers flink =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder()
                                    .numTaskManagers(1)
                                    // enable checkpointing for the UpsertTestSink to write anything
                                    .setConfigOption(
                                            ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                                            Duration.ofMillis(500))
                                    .build())
                    .withTestcontainersSettings(
                            TestcontainersSettings.builder()
                                    .network(NETWORK)
                                    .logger(LOG)
                                    .dependsOn(KAFKA)
                                    .build())
                    .build();

    @TempDir private File tempDir;

    @BeforeEach
    void setup() throws Exception {
        flink.start();
    }

    @AfterEach
    void tearDown() {
        flink.stop();
    }

    @Test
    void testUpsert() throws Exception {
        String outputFilepath = "/flink/records-upsert.out";

        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE TABLE UpsertSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT,",
                        "    PRIMARY KEY (user_id) NOT ENFORCED",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO UpsertSinkTable(",
                        "  SELECT user_id, user_name, COUNT(*) AS user_count",
                        "  FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))",
                        "    AS UserCountTable(user_id, user_name)",
                        "  GROUP BY user_id, user_name);");
        executeSql(sqlLines);

        /*
        | user_id | user_name | user_count |
        | ------- | --------- | ---------- |
        |    1    |    Bob    |     2      |
        |   22    |    Tom    |     1      |
        |   42    |    Kim    |     3      |
        */

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testAppendNoKey() throws Exception {
        String outputFilepath = "/flink/records-append.out";

        List<String> sqlLines =
                Arrays.asList(
                        "SET 'execution.runtime-mode' = 'batch';",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO AppendSinkTable",
                        "  SELECT *",
                        "  FROM (",
                        "    VALUES",
                        "      (1, 'Bob', CAST(0 AS BIGINT)),",
                        "      (22, 'Tom', CAST(0 AS BIGINT)),",
                        "      (42, 'Kim', CAST(0 AS BIGINT)),",
                        "      (42, 'Kim', CAST(0 AS BIGINT)),",
                        "      (42, 'Kim', CAST(0 AS BIGINT)),",
                        "      (1, 'Bob', CAST(0 AS BIGINT)))",
                        "    AS UserCountTable(user_id, user_name, user_count);");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 3);
    }

    @Test
    void testMatchRecognize() throws Exception {
        String outputFilepath = "/flink/records-matchrecognize.out";

        String[] messages =
                new String[] {
                    "{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"timestamp\": \"2018-03-12T08:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}",
                    "{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}",
                    "{\"timestamp\": \"2018-03-12T10:40:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}"
                };
        sendMessages("test-json", messages);

        List<String> sqlLines =
                Arrays.asList(
                        "CREATE FUNCTION RegReplace AS 'org.apache.flink.table.toolbox.StringRegexReplaceFunction';",
                        "",
                        "CREATE TABLE JsonSourceTable (",
                        "    `timestamp` TIMESTAMP_LTZ(3),",
                        "    `user` STRING,",
                        "    `event` ROW<type STRING, message STRING>,",
                        "    `rowtime` AS `timestamp`,",
                        "    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '2' SECOND",
                        ") WITH (",
                        "    'connector' = 'kafka',",
                        "    'topic' = 'test-json',",
                        "    'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        "    'scan.startup.mode' = 'earliest-offset',",
                        "    'format' = 'json',",
                        "    'json.timestamp-format.standard' = 'ISO-8601'",
                        ");",
                        "",
                        "CREATE TABLE AppendSinkTable (",
                        "    user_id INT,",
                        "    user_name STRING,",
                        "    user_count BIGINT",
                        "  ) WITH (",
                        "    'connector' = 'upsert-files',",
                        "    'key.format' = 'json',",
                        "    'value.format' = 'json',",
                        "    'output-filepath' = '" + outputFilepath + "'",
                        "  );",
                        "",
                        "INSERT INTO AppendSinkTable",
                        "  SELECT 1 as user_id, T.userName as user_name, cast(1 as BIGINT) as user_count",
                        "  FROM (",
                        "    SELECT `user`, `rowtime`",
                        "    FROM JsonSourceTable",
                        "    WHERE `user` IS NOT NULL)",
                        "  MATCH_RECOGNIZE (",
                        "    ORDER BY rowtime",
                        "    MEASURES",
                        "        `user` as userName",
                        "    PATTERN (A)",
                        "    DEFINE",
                        "        A as `user` = 'Alice'",
                        "  ) as T;");
        executeSql(sqlLines);

        verifyNumberOfResultRecords(outputFilepath, 1);
    }

    // duplicated from KafkaContainerClient@flink-end-to-end-tests-common
    private void sendMessages(String topic, String... messages) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        final int numPartitions = 1;
        final short replicationFactor = 1;
        try (AdminClient admin = AdminClient.create(props)) {
            admin.createTopics(
                            Collections.singletonList(
                                    new NewTopic(topic, numPartitions, replicationFactor)))
                    .all()
                    .get();
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Fail to create topic [%s partitions: %d replicas: %d].",
                            topic, numPartitions, replicationFactor),
                    e);
        }

        try (Producer<Bytes, String> producer =
                new KafkaProducer<>(props, new BytesSerializer(), new StringSerializer())) {
            for (String message : messages) {
                producer.send(new ProducerRecord<>(topic, message));
            }
        }
    }

    private void verifyNumberOfResultRecords(String resultFilePath, int expectedNumberOfRecords)
            throws IOException, InterruptedException {
        File tempOutputFile = new File(tempDir, "records.out");
        String tempOutputFilepath = tempOutputFile.toString();
        GenericContainer<?> taskManager = flink.getTaskManagers().get(0);
        int numberOfResultRecords;
        while (true) {
            Thread.sleep(50); // prevent NotFoundException: Status 404
            try {
                taskManager.copyFileFromContainer(resultFilePath, tempOutputFilepath);
                numberOfResultRecords = UpsertTestFileUtil.getNumberOfRecords(tempOutputFile);
                if (numberOfResultRecords == expectedNumberOfRecords) {
                    break;
                }
            } catch (Exception ignored) {
            }
        }
        assertThat(numberOfResultRecords).isEqualTo(expectedNumberOfRecords);
    }

    private void executeSql(List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorUpsertTestJar, sqlConnectorKafkaJar, sqlToolBoxJar)
                        .build());
    }
}
