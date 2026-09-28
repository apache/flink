/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.schema.registry.test;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.connector.upserttest.sink.UpsertTestFileUtil;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.FileUtils;
import org.apache.flink.test.util.JobSubmission;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.util.DockerImageVersions;

import example.avro.EventType;
import example.avro.User;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for the Confluent Schema Registry Avro formats against a real registry.
 *
 * <p>Records travel through files rather than Kafka topics so that the test does not depend on the
 * externalized Kafka connector. Kafka only serves as the registry's storage. The test side uses
 * Confluent's own serializer and deserializer, so the wire format is checked in both directions.
 */
class ConfluentSchemaRegistryITCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentSchemaRegistryITCase.class);

    private static final String REGISTRY_ALIAS = "registry";
    private static final int REGISTRY_PORT = 8081;
    private static final String REGISTRY_URL_IN_NETWORK =
            "http://" + REGISTRY_ALIAS + ":" + REGISTRY_PORT;
    private static final String KAFKA_LISTENER = "kafka:19092";

    private static final String INPUT_TOPIC = "test-avro-input";
    private static final String OUTPUT_SUBJECT = "test-output-subject";
    private static final String SQL_SUBJECT = "sql-users-value";
    private static final String CONTAINER_INPUT = "/tmp/confluent-input.txt";
    private static final String CONTAINER_OUTPUT_DIR = "/tmp/";
    private static final String CONTAINER_SQL_OUTPUT = "/tmp/confluent-sql.out";

    private static final List<User> USERS =
            List.of(
                    user("Alyssa", "250", "green"),
                    user("Charlie", "10", "blue"),
                    user("Ben", "7", "red"));

    private static final Network NETWORK = Network.newNetwork();

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withListener(KAFKA_LISTENER)
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("kafka"));

    private static final GenericContainer<?> REGISTRY =
            new GenericContainer<>(DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(REGISTRY_ALIAS)
                    .withExposedPorts(REGISTRY_PORT)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", REGISTRY_ALIAS)
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + REGISTRY_PORT)
                    .withEnv(
                            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            "PLAINTEXT://" + KAFKA_LISTENER)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                    .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("registry"));

    private static FlinkContainers flink;

    private static SchemaRegistryClient registryClient;
    private static String registryUrl;

    @TempDir private static Path tempDir;

    @BeforeAll
    static void startContainers() throws Exception {
        KAFKA.start();
        REGISTRY.start();
        // Workaround for FLINK-36454: FlinkContainers writes config.yaml from these settings alone,
        // which drops the distribution's JVM options that bin/flink needs on Java 17
        final Configuration distConfig =
                GlobalConfiguration.loadConfiguration(
                        FileUtils.findFlinkDist().resolve("conf").toString());
        flink =
                FlinkContainers.builder()
                        .withFlinkContainersSettings(
                                FlinkContainersSettings.builder()
                                        .numTaskManagers(1)
                                        // fail fast instead of restarting when the registry is
                                        // unreachable or a record cannot be (de)serialized
                                        .setConfigOption(
                                                RestartStrategyOptions.RESTART_STRATEGY, "none")
                                        .setConfigOption(
                                                CoreOptions.FLINK_JVM_OPTIONS,
                                                distConfig.get(CoreOptions.FLINK_JVM_OPTIONS))
                                        .build())
                        .withTestcontainersSettings(
                                TestcontainersSettings.builder()
                                        .network(NETWORK)
                                        .logger(LOG)
                                        .build())
                        .build();
        flink.start();
        registryUrl = "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(REGISTRY_PORT);
        registryClient = new CachedSchemaRegistryClient(registryUrl, 10);
    }

    @AfterAll
    static void stopContainers() {
        if (flink != null) {
            flink.stop();
        }
        REGISTRY.stop();
        KAFKA.stop();
        NETWORK.close();
    }

    @Test
    void testDataStreamJob() throws Exception {
        // not closed: closing a Confluent serde also closes the shared registry client
        final KafkaAvroSerializer serializer =
                new KafkaAvroSerializer(registryClient, Map.of("schema.registry.url", registryUrl));
        final List<String> lines = new ArrayList<>();
        for (User user : USERS) {
            lines.add(Base64.getEncoder().encodeToString(serializer.serialize(INPUT_TOPIC, user)));
        }
        final File input = tempDir.resolve("input.txt").toFile();
        Files.write(input.toPath(), lines);
        copyToCluster(input, CONTAINER_INPUT);

        final JobID jobId =
                flink.submitJob(
                        new JobSubmission.JobSubmissionBuilder(
                                        ResourceTestUtils.getResource(
                                                ".*/TestAvroConsumerConfluent\\.jar"))
                                .setDetached(true)
                                .addArgument("--input-path", "file://" + CONTAINER_INPUT)
                                .addArgument("--output-dir", CONTAINER_OUTPUT_DIR)
                                .addArgument("--schema-registry-url", REGISTRY_URL_IN_NETWORK)
                                .addArgument("--output-subject", OUTPUT_SUBJECT)
                                .build());
        waitForSuccess(jobId);

        assertThat(
                        readOutput(
                                CONTAINER_OUTPUT_DIR + TestAvroConsumerConfluent.STRING_OUTPUT,
                                new SimpleStringSchema()))
                .isEqualTo(byName(USERS));

        final KafkaAvroDeserializer specificDeserializer =
                new KafkaAvroDeserializer(
                        registryClient,
                        Map.of("schema.registry.url", registryUrl, "specific.avro.reader", true));
        assertThat(
                        readOutput(
                                        CONTAINER_OUTPUT_DIR
                                                + TestAvroConsumerConfluent.AVRO_OUTPUT,
                                        new BytesSchema())
                                .values())
                .map(bytes -> specificDeserializer.deserialize(OUTPUT_SUBJECT, bytes))
                .containsExactlyInAnyOrderElementsOf(USERS);
        assertThat(registryClient.getAllVersions(OUTPUT_SUBJECT)).hasSize(1);
        assertThat(
                        new Schema.Parser()
                                .parse(
                                        registryClient
                                                .getLatestSchemaMetadata(OUTPUT_SUBJECT)
                                                .getSchema()))
                .isEqualTo(User.getClassSchema());

        final Schema evolved = TestAvroConsumerConfluent.evolvedUserSchema();
        final List<GenericRecord> expectedEvolved = new ArrayList<>();
        for (User user : USERS) {
            expectedEvolved.add(
                    new GenericRecordBuilder(evolved)
                            .set("name", user.getName())
                            .set("favoriteNumber", user.getFavoriteNumber())
                            .set("favoriteColor", user.getFavoriteColor())
                            .set(
                                    "eventType",
                                    new GenericData.EnumSymbol(
                                            evolved.getField("eventType").schema(),
                                            user.getEventType().name()))
                            .build());
        }
        assertThat(
                        readOutput(
                                CONTAINER_OUTPUT_DIR + TestAvroConsumerConfluent.EVOLVED_OUTPUT,
                                new SimpleStringSchema()))
                .isEqualTo(byName(expectedEvolved));
    }

    @Test
    void testSqlWriteWithShadedFormat() throws Exception {
        final Set<JobID> existingJobs = listJobIds();

        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(
                                List.of(
                                        "CREATE TABLE users (",
                                        "  name STRING,",
                                        "  favorite_number STRING,",
                                        "  favorite_color STRING,",
                                        "  PRIMARY KEY (name) NOT ENFORCED",
                                        ") WITH (",
                                        "  'connector' = 'upsert-files',",
                                        "  'output-filepath' = '" + CONTAINER_SQL_OUTPUT + "',",
                                        "  'key.format' = 'json',",
                                        "  'value.format' = 'avro-confluent',",
                                        "  'value.avro-confluent.url' = '"
                                                + REGISTRY_URL_IN_NETWORK
                                                + "',",
                                        "  'value.avro-confluent.subject' = '" + SQL_SUBJECT + "'",
                                        ");",
                                        "INSERT INTO users VALUES",
                                        "  ('Alyssa', '250', 'green'),",
                                        "  ('Charlie', '10', 'blue'),",
                                        "  ('Ben', '7', 'red');"))
                        .addJars(
                                ResourceTestUtils.getResource(
                                        ".*/sql-jars/flink-sql-avro-confluent-registry.*\\.jar"),
                                ResourceTestUtils.getResource(
                                        ".*/sql-jars/flink-test-utils.*\\.jar"))
                        .build());

        final Set<JobID> newJobs = listJobIds();
        newJobs.removeAll(existingJobs);
        // the SQL client reads the script from stdin and exits 0 even if a statement fails
        assertThat(newJobs)
                .as("SQL job submitted; see the SQL client output in the log if empty")
                .hasSize(1);
        waitForSuccess(newJobs.iterator().next());

        final KafkaAvroDeserializer deserializer =
                new KafkaAvroDeserializer(
                        registryClient, Map.of("schema.registry.url", registryUrl));
        assertThat(readOutput(CONTAINER_SQL_OUTPUT, new BytesSchema()).values())
                .map(bytes -> (GenericRecord) deserializer.deserialize(SQL_SUBJECT, bytes))
                .map(
                        record ->
                                record.get("name")
                                        + ","
                                        + record.get("favorite_number")
                                        + ","
                                        + record.get("favorite_color"))
                .containsExactlyInAnyOrder("Alyssa,250,green", "Charlie,10,blue", "Ben,7,red");
        assertThat(registryClient.getAllVersions(SQL_SUBJECT)).hasSize(1);
    }

    private static User user(String name, String favoriteNumber, String favoriteColor) {
        return User.newBuilder()
                .setName(name)
                .setFavoriteNumber(favoriteNumber)
                .setFavoriteColor(favoriteColor)
                .setEventType(EventType.meeting)
                .build();
    }

    private static Map<String, String> byName(List<? extends GenericRecord> records) {
        return records.stream()
                .collect(
                        Collectors.toMap(
                                record -> record.get("name").toString(), GenericRecord::toString));
    }

    private static void copyToCluster(File file, String containerPath) {
        final MountableFile mountableFile = MountableFile.forHostPath(file.toPath());
        flink.getJobManager().copyFileToContainer(mountableFile, containerPath);
        for (GenericContainer<?> taskManager : flink.getTaskManagers()) {
            taskManager.copyFileToContainer(mountableFile, containerPath);
        }
    }

    private static <V> Map<String, V> readOutput(
            String containerPath, DeserializationSchema<V> valueSchema) throws Exception {
        final File local = tempDir.resolve(new File(containerPath).getName()).toFile();
        flink.getTaskManagers()
                .get(0)
                .copyFileFromContainer(containerPath, local.getAbsolutePath());
        return UpsertTestFileUtil.readRecords(local, new SimpleStringSchema(), valueSchema);
    }

    private static Set<JobID> listJobIds() throws Exception {
        final Collection<JobStatusMessage> jobs = flink.getRestClusterClient().listJobs().get();
        return jobs.stream().map(JobStatusMessage::getJobId).collect(Collectors.toSet());
    }

    private static void waitForSuccess(JobID jobId) throws Exception {
        final JobResult result = flink.getRestClusterClient().requestJobResult(jobId).get();
        if (!result.isSuccess()) {
            throw new AssertionError(
                    "Job " + jobId + " did not succeed: " + result.getJobStatus().orElse(null),
                    result.getSerializedThrowable().orElse(null));
        }
    }

    /** Passes the raw record bytes through. */
    private static final class BytesSchema implements DeserializationSchema<byte[]> {

        @Override
        public byte[] deserialize(byte[] message) {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }
    }
}
