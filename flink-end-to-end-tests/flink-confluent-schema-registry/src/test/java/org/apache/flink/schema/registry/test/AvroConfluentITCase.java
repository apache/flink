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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** E2E Test for Avro-Confluent integration. */
@Testcontainers
public class AvroConfluentITCase {

    private static final Logger LOG = LoggerFactory.getLogger(AvroConfluentITCase.class);

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final String INTER_CONTAINER_KAFKA_BOOTSTRAP =
            INTER_CONTAINER_KAFKA_ALIAS + ":19092";
    private static final String INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS = "schema-registry";
    private static final String INTER_CONTAINER_SCHEMA_REGISTRY_URL =
            "http://" + INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS + ":8081";

    private static final String TOPIC = "test-avro-input";
    private static final String RESULT_TOPIC = "test-avro-output";
    private static final String MANUAL_TOPIC = "test-avro-input-manual";
    private static final String MANUAL_RESULT_TOPIC = "test-avro-output-manual";
    private static final String MISSING_SCHEMA_TOPIC = "test-avro-output-missing-schema";

    private static final String INPUT_SCHEMA = SchemaLoader.loadSchema("avro/input-record.avsc");
    private static final String OUTPUT_SCHEMA = SchemaLoader.loadSchema("avro/output-record.avsc");

    private static final Path sqlToolBoxJar = ResourceTestUtils.getResource(".*/SqlToolbox\\.jar");

    private final Path sqlConnectorKafkaJar = ResourceTestUtils.getResource(".*kafka.*\\.jar");
    private final Path sqlConnectorUpsertTestJar =
            ResourceTestUtils.getResource(".*flink-test-utils.*\\.jar");
    private final Path sqlAvroConfluentJar =
            ResourceTestUtils.getResource(".*avro-confluent.*\\.jar");

    private static final Network NETWORK = Network.newNetwork();

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    @Container
    public static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withListener(INTER_CONTAINER_KAFKA_BOOTSTRAP);

    @Container
    private static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", INTER_CONTAINER_SCHEMA_REGISTRY_ALIAS)
                    .withEnv(
                            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            INTER_CONTAINER_KAFKA_BOOTSTRAP)
                    .dependsOn(KAFKA);

    @RegisterExtension
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

    @BeforeEach
    public void before() throws Exception {
        // Create topics using external bootstrap servers since we're outside the Docker network
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            List<NewTopic> topics =
                    Arrays.asList(
                            new NewTopic(TOPIC, 1, (short) 1),
                            new NewTopic(RESULT_TOPIC, 1, (short) 1),
                            new NewTopic(MANUAL_TOPIC, 1, (short) 1),
                            new NewTopic(MANUAL_RESULT_TOPIC, 1, (short) 1),
                            new NewTopic(MISSING_SCHEMA_TOPIC, 1, (short) 1));
            admin.createTopics(topics);

            // Poll for topic creation with timeout
            try {
                CommonTestUtils.waitUntilIgnoringExceptions(
                        () -> {
                            try {
                                Set<String> existingTopics = admin.listTopics().names().get();
                                return topics.stream()
                                        .allMatch(topic -> existingTopics.contains(topic.name()));
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

            LOG.info("Topics created successfully");
        }
    }

    @Test
    public void testAvroConfluentIntegrationWithAutoRegister() throws Exception {
        List<String> sqlLines =
                initializeSqlLines(
                        "avro_confluent_roundtrip_e2e.sql",
                        generateRoundtripReplaceVars(TOPIC, RESULT_TOPIC, true));

        executeSql(sqlLines);

        verifyNumberOfResultRecords(RESULT_TOPIC, 3);
    }

    @Test
    public void testAvroConfluentIntegrationWithManualRegister() throws Exception {
        // Manually register schemas before creating tables
        registerSchema(MANUAL_TOPIC + "-value", INPUT_SCHEMA);
        registerSchema(MANUAL_RESULT_TOPIC + "-value", OUTPUT_SCHEMA);

        List<String> sqlLines =
                initializeSqlLines(
                        "avro_confluent_roundtrip_e2e.sql",
                        generateRoundtripReplaceVars(MANUAL_TOPIC, MANUAL_RESULT_TOPIC, false));

        executeSql(sqlLines);

        verifyNumberOfResultRecords(MANUAL_RESULT_TOPIC, 3);

        // The job must not have registered any new schema version on top of the
        // manually registered ones
        assertThat(getSubjectVersions(MANUAL_TOPIC + "-value")).containsExactly(1);
        assertThat(getSubjectVersions(MANUAL_RESULT_TOPIC + "-value")).containsExactly(1);
    }

    @Test
    public void testWritingFailsWhenAutoRegisterDisabledAndSchemaMissing() throws Exception {
        // No schema is registered for MISSING_SCHEMA_TOPIC, so writing with
        // auto.register.schemas=false must fail instead of registering the schema
        Map<String, String> vars = new HashMap<>();
        vars.put("$OUTPUT_TOPIC", MISSING_SCHEMA_TOPIC);
        vars.put("$BOOTSTRAP_SERVERS", INTER_CONTAINER_KAFKA_BOOTSTRAP);
        vars.put("$SCHEMA_REGISTRY_URL", INTER_CONTAINER_SCHEMA_REGISTRY_URL);
        List<String> sqlLines = initializeSqlLines("avro_confluent_missing_schema_e2e.sql", vars);

        // The SQL client reads the script from stdin and always exits successfully, even if
        // a statement fails; the failure must be observed through the job status instead.
        // 'table.dml-sync' guarantees the job has reached a terminal state afterwards.
        executeSql(sqlLines);

        JobStatusMessage insertJob =
                FLINK.getRestClusterClient().listJobs().get().stream()
                        .filter(job -> job.getJobName().contains("avro_output_missing_schema"))
                        .findAny()
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Could not find the job writing to the missing-schema table"));
        assertThat(insertJob.getJobState()).isEqualTo(JobStatus.FAILED);

        // The failed job must not have registered the schema either
        assertThat(subjectExists(MISSING_SCHEMA_TOPIC + "-value")).isFalse();
    }

    private static Map<String, String> generateRoundtripReplaceVars(
            String inputTopic, String outputTopic, boolean autoRegisterSchemas) {
        Map<String, String> vars = new HashMap<>();
        vars.put("$INPUT_TOPIC", inputTopic);
        vars.put("$OUTPUT_TOPIC", outputTopic);
        vars.put("$BOOTSTRAP_SERVERS", INTER_CONTAINER_KAFKA_BOOTSTRAP);
        vars.put("$SCHEMA_REGISTRY_URL", INTER_CONTAINER_SCHEMA_REGISTRY_URL);
        vars.put("$AUTO_REGISTER_SCHEMAS", String.valueOf(autoRegisterSchemas));
        return vars;
    }

    private static List<String> initializeSqlLines(String sqlPath, Map<String, String> vars)
            throws IOException {
        URL url = AvroConfluentITCase.class.getClassLoader().getResource(sqlPath);
        if (url == null) {
            throw new FileNotFoundException(sqlPath);
        }
        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }
        return result;
    }

    private static String getSchemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    private String registerSchema(String subject, String schema) {
        try {
            // Use Jackson to properly serialize the request body
            Map<String, String> requestBodyMap = new HashMap<>();
            requestBodyMap.put("schema", schema);
            String requestBody = OBJECT_MAPPER.writeValueAsString(requestBodyMap);

            LOG.info("Registering schema for subject {} with schema: {}", subject, schema);

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(
                                    URI.create(
                                            getSchemaRegistryUrl()
                                                    + "/subjects/"
                                                    + subject
                                                    + "/versions"))
                            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                            .build();

            HttpResponse<String> response =
                    HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException(
                        "Failed to register schema for subject "
                                + subject
                                + ": "
                                + response.statusCode()
                                + " - "
                                + response.body());
            }

            LOG.info("Successfully registered schema for subject {}: {}", subject, response.body());
            return response.body();
        } catch (Exception e) {
            throw new RuntimeException("Failed to register schema for subject " + subject, e);
        }
    }

    private List<Integer> getSubjectVersions(String subject) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(
                                    URI.create(
                                            getSchemaRegistryUrl()
                                                    + "/subjects/"
                                                    + subject
                                                    + "/versions"))
                            .GET()
                            .build();
            HttpResponse<String> response =
                    HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException(
                        "Failed to get versions for subject "
                                + subject
                                + ": "
                                + response.statusCode()
                                + " - "
                                + response.body());
            }
            return Arrays.asList(OBJECT_MAPPER.readValue(response.body(), Integer[].class));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get versions for subject " + subject, e);
        }
    }

    private boolean subjectExists(String subject) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(
                                    URI.create(
                                            getSchemaRegistryUrl()
                                                    + "/subjects/"
                                                    + subject
                                                    + "/versions"))
                            .GET()
                            .build();
            HttpResponse<String> response =
                    HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            throw new RuntimeException("Failed to check subject " + subject, e);
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
        props.put("schema.registry.url", getSchemaRegistryUrl());
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
        LOG.info("Submitting SQL statements: {}", String.join("\n", sqlLines));
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
