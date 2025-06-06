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

package org.apache.flink.schema.registry.test;

import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** E2E Test for Avro-Confluent integration. */
@Testcontainers
public class AvroConfluentITCase {

    private static final Logger LOG = LoggerFactory.getLogger(AvroConfluentITCase.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final String INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS = "schema-registry";

    private static final String TOPIC = "test-avro-input";
    private static final String RESULT_TOPIC = "test-avro-output";
    private static final String MANUAL_TOPIC = "test-avro-input-manual";
    private static final String MANUAL_RESULT_TOPIC = "test-avro-output-manual";

    private static final String INPUT_SCHEMA = SchemaLoader.loadSchema("avro/input-record.avsc");
    private static final String OUTPUT_SCHEMA = SchemaLoader.loadSchema("avro/output-record.avsc");

    private static final Path sqlToolBoxJar = ResourceTestUtils.getResource(".*/SqlToolbox\\.jar");

    private final Path sqlConnectorKafkaJar = ResourceTestUtils.getResource(".*kafka.*\\.jar");
    private final Path sqlConnectorUpsertTestJar =
            ResourceTestUtils.getResource(".*flink-test-utils.*\\.jar");
    private final Path sqlAvroConfluentJar =
            ResourceTestUtils.getResource(".*avro-confluent.*\\.jar");

    private static final Network NETWORK = Network.newNetwork();

    @Container
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Container
    private static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS)
                    .withEnv(
                            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            INTER_CONTAINER_KAFKA_ALIAS + ":9092")
                    .dependsOn(KAFKA);

    private static final FlinkContainers FLINK =
            FlinkContainers.builder()
                    .withFlinkContainersSettings(
                            FlinkContainersSettings.builder().numTaskManagers(1).build())
                    .withTestcontainersSettings(
                            TestcontainersSettings.builder()
                                    .network(NETWORK)
                                    .logger(LOG)
                                    .dependsOn(KAFKA)
                                    .build())
                    .build();

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();

    @BeforeAll
    public static void setup() throws Exception {
        KAFKA.start();
        SCHEMA_REGISTRY.start();
        FLINK.start();
    }

    @AfterAll
    public static void tearDown() {
        FLINK.stop();
        SCHEMA_REGISTRY.stop();
        KAFKA.stop();
    }

    @BeforeEach
    public void before() throws Exception {
        // Create topics using external bootstrap servers since we're outside the Docker network
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(
                    Arrays.asList(
                            new NewTopic(TOPIC, 1, (short) 1),
                            new NewTopic(RESULT_TOPIC, 1, (short) 1),
                            new NewTopic(MANUAL_TOPIC, 1, (short) 1),
                            new NewTopic(MANUAL_RESULT_TOPIC, 1, (short) 1)));

            // Poll for topic creation with timeout
            try {
                CommonTestUtils.waitUntilIgnoringExceptions(
                        () -> {
                            try {
                                Set<String> topics = admin.listTopics().names().get();
                                return topics.contains(TOPIC)
                                        && topics.contains(RESULT_TOPIC)
                                        && topics.contains(MANUAL_TOPIC)
                                        && topics.contains(MANUAL_RESULT_TOPIC);
                            } catch (Exception e) {
                                LOG.warn("Exception while checking topic creation", e);
                                return false;
                            }
                        },
                        Duration.ofSeconds(30),
                        Duration.ofMillis(100),
                        "Topics were not created in time");
            } catch (TimeoutException | InterruptedException e) {
                throw new RuntimeException("Failed to wait for topic creation", e);
            }

            LOG.info(
                    "Topics {}, {}, {}, and {} created successfully",
                    TOPIC,
                    RESULT_TOPIC,
                    MANUAL_TOPIC,
                    MANUAL_RESULT_TOPIC);
        }
    }

    @Test
    public void testAvroConfluentIntegrationWithAutoRegister() throws Exception {
        // Combine all SQL statements into a single submission
        List<String> allSqlStatements =
                Arrays.asList(
                        "SET 'table.dml-sync' = 'true';",
                        "",
                        "CREATE TABLE avro_input (",
                        "  name STRING,",
                        "  favoriteNumber STRING,",
                        "  favoriteColor STRING,",
                        "  eventType STRING",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = '" + TOPIC + "',",
                        "  'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        "  'properties.group.id' = 'test-group',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'scan.bounded.mode' = 'latest-offset',",
                        "  'format' = 'avro-confluent',",
                        "  'avro-confluent.url' = 'http://"
                                + INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS
                                + ":8081',",
                        "  'avro-confluent.auto.register.schemas' = 'true'",
                        ");",
                        "",
                        "CREATE TABLE avro_output (",
                        "  name STRING,",
                        "  favoriteNumber STRING,",
                        "  favoriteColor STRING,",
                        "  eventType STRING",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = '" + RESULT_TOPIC + "',",
                        "  'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        "  'properties.group.id' = 'test-group',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'scan.bounded.mode' = 'latest-offset',",
                        "  'format' = 'avro-confluent',",
                        "  'avro-confluent.url' = 'http://"
                                + INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS
                                + ":8081',",
                        "  'avro-confluent.auto.register.schemas' = 'true'",
                        ");",
                        "",
                        "INSERT INTO avro_input VALUES",
                        "  ('Alice', '42', 'blue', 'INSERT'),",
                        "  ('Bob', '7', 'red', 'INSERT'),",
                        "  ('Charlie', '73', 'green', 'INSERT');",
                        "",
                        "INSERT INTO avro_output",
                        "SELECT * FROM avro_input;");

        LOG.info("Submitting SQL statements: {}", String.join("\n", allSqlStatements));

        // Execute all SQL statements in a single submission
        executeSql(allSqlStatements);

        // Verify output
        verifyNumberOfResultRecords(RESULT_TOPIC, 3);
    }

    @Test
    public void testAvroConfluentIntegrationWithManualRegister() throws Exception {
        // Manually register schemas before creating tables
        LOG.info("Registering schemas manually for manual registration test");
        registerSchema(MANUAL_TOPIC + "-value", INPUT_SCHEMA);
        registerSchema(MANUAL_RESULT_TOPIC + "-value", OUTPUT_SCHEMA);

        // Execute all statements in a single SQL session to maintain table state
        List<String> allSqlStatements =
                Arrays.asList(
                        "SET 'table.dml-sync' = 'true';",
                        "",
                        "CREATE TABLE avro_input_manual (",
                        "  name STRING,",
                        "  favoriteNumber STRING,",
                        "  favoriteColor STRING,",
                        "  eventType STRING",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = '" + MANUAL_TOPIC + "',",
                        "  'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        "  'properties.group.id' = 'test-group-manual',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'scan.bounded.mode' = 'latest-offset',",
                        "  'format' = 'avro-confluent',",
                        "  'avro-confluent.url' = 'http://"
                                + INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS
                                + ":8081',",
                        "  'avro-confluent.auto.register.schemas' = 'false'",
                        ");",
                        "",
                        "CREATE TABLE avro_output_manual (",
                        "  name STRING,",
                        "  favoriteNumber STRING,",
                        "  favoriteColor STRING,",
                        "  eventType STRING",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = '" + MANUAL_RESULT_TOPIC + "',",
                        "  'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        "  'properties.group.id' = 'test-group-manual',",
                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'scan.bounded.mode' = 'latest-offset',",
                        "  'format' = 'avro-confluent',",
                        "  'avro-confluent.url' = 'http://"
                                + INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS
                                + ":8081',",
                        "  'avro-confluent.auto.register.schemas' = 'false'",
                        ");",
                        "",
                        "INSERT INTO avro_input_manual VALUES",
                        "  ('David', '25', 'yellow', 'INSERT'),",
                        "  ('Eve', '30', 'purple', 'INSERT'),",
                        "  ('Frank', '45', 'orange', 'INSERT');",
                        "",
                        "INSERT INTO avro_output_manual",
                        "SELECT * FROM avro_input_manual;");

        LOG.info("Executing all SQL statements for manual registration test in single session");
        executeSql(allSqlStatements);

        // Verify output
        verifyNumberOfResultRecords(MANUAL_RESULT_TOPIC, 3);
    }

    private String registerSchema(String subject, String schema) {
        try {
            // Use Jackson to properly serialize the request body
            Map<String, String> requestBodyMap = new HashMap<>();
            requestBodyMap.put("schema", schema);
            String requestBody = objectMapper.writeValueAsString(requestBodyMap);

            LOG.info("Registering schema for subject {} with schema: {}", subject, schema);

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(
                                    URI.create(
                                            "http://"
                                                    + SCHEMA_REGISTRY.getHost()
                                                    + ":"
                                                    + SCHEMA_REGISTRY.getMappedPort(8081)
                                                    + "/subjects/"
                                                    + subject
                                                    + "/versions"))
                            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                            .build();

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                LOG.error(
                        "Failed to register schema for subject {}: {} - {}",
                        subject,
                        response.statusCode(),
                        response.body());
                throw new RuntimeException(
                        "Failed to register schema: "
                                + response.statusCode()
                                + " - "
                                + response.body());
            }

            LOG.info("Successfully registered schema for subject {}: {}", subject, response.body());
            return response.body();
        } catch (Exception e) {
            LOG.error("Error registering schema for subject {}: {}", subject, e.getMessage(), e);
            throw new RuntimeException("Failed to register schema", e);
        }
    }

    private void verifyNumberOfResultRecords(String topicName, int expectedCount) throws Exception {
        // Use external bootstrap servers since we're outside the Docker network
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-verification-group-" + topicName);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(
                "schema.registry.url",
                "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            List<GenericRecord> records = new ArrayList<>();

            // Poll for records until we have enough or timeout
            try {
                CommonTestUtils.waitUntilIgnoringExceptions(
                        () -> {
                            try {
                                ConsumerRecords<String, GenericRecord> consumerRecords =
                                        consumer.poll(Duration.ofMillis(100));
                                for (ConsumerRecord<String, GenericRecord> record :
                                        consumerRecords) {
                                    records.add(record.value());
                                }
                                return records.size() >= expectedCount;
                            } catch (Exception e) {
                                LOG.warn("Exception while polling records", e);
                                return false;
                            }
                        },
                        Duration.ofSeconds(30),
                        Duration.ofMillis(100),
                        "Expected "
                                + expectedCount
                                + " records but only found "
                                + records.size()
                                + " in topic "
                                + topicName);
            } catch (TimeoutException | InterruptedException e) {
                throw new RuntimeException("Failed to wait for records in topic " + topicName, e);
            }

            // Verify the exact number of records
            assertEquals(
                    expectedCount,
                    records.size(),
                    "Expected exactly " + expectedCount + " records but found " + records.size());

            // Log the records for debugging
            for (GenericRecord record : records) {
                LOG.info("Found record in {}: {}", topicName, record);
            }
        }
    }

    private void executeSql(List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(
                                sqlConnectorUpsertTestJar,
                                sqlConnectorKafkaJar,
                                sqlAvroConfluentJar,
                                sqlToolBoxJar)
                        .build());
    }
}
