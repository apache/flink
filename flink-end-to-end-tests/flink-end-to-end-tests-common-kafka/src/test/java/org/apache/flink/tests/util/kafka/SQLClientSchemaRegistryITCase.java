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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;
import org.apache.flink.tests.util.kafka.containers.SchemaRegistryContainer;
import org.apache.flink.util.DockerImageVersions;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/** End-to-end test for SQL client using Avro Confluent Registry format. */
public class SQLClientSchemaRegistryITCase {
    private static final Logger LOG = LoggerFactory.getLogger(SQLClientSchemaRegistryITCase.class);
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);

    public static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    public static final String INTER_CONTAINER_REGISTRY_ALIAS = "registry";
    private static final Path sqlAvroJar = ResourceTestUtils.getResource(".*avro.jar");
    private static final Path sqlAvroRegistryJar =
            ResourceTestUtils.getResource(".*avro-confluent.jar");
    private static final Path sqlToolBoxJar = ResourceTestUtils.getResource(".*SqlToolbox.jar");
    private final Path sqlConnectorKafkaJar = ResourceTestUtils.getResource(".*kafka.jar");

    @ClassRule public static final Network NETWORK = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @ClassRule
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withLogConsumer(LOG_CONSUMER);

    @ClassRule
    public static final SchemaRegistryContainer REGISTRY =
            new SchemaRegistryContainer(DockerImageName.parse(DockerImageVersions.SCHEMA_REGISTRY))
                    .withKafka(INTER_CONTAINER_KAFKA_ALIAS + ":9092")
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_REGISTRY_ALIAS)
                    .dependsOn(KAFKA);

    public final TestcontainersSettings testcontainersSettings =
            TestcontainersSettings.builder().network(NETWORK).logger(LOG).dependsOn(KAFKA).build();

    public final FlinkContainers flink =
            FlinkContainers.builder().withTestcontainersSettings(testcontainersSettings).build();

    private KafkaContainerClient kafkaClient;
    private CachedSchemaRegistryClient registryClient;

    @Before
    public void setUp() throws Exception {
        flink.start();
        kafkaClient = new KafkaContainerClient(KAFKA);
        registryClient = new CachedSchemaRegistryClient(REGISTRY.getSchemaRegistryUrl(), 10);
    }

    @After
    public void tearDown() {
        flink.stop();
    }

    @Test
    public void testReading() throws Exception {
        String testCategoryTopic = "test-category-" + UUID.randomUUID().toString();
        String testResultsTopic = "test-results-" + UUID.randomUUID().toString();
        kafkaClient.createTopic(1, 1, testCategoryTopic);
        Schema categoryRecord =
                SchemaBuilder.record("org.apache.flink.avro.generated.record")
                        .fields()
                        .requiredLong("category_id")
                        .optionalString("name")
                        .endRecord();
        String categorySubject = testCategoryTopic + "-value";
        registryClient.register(categorySubject, new AvroSchema(categoryRecord));
        GenericRecordBuilder categoryBuilder = new GenericRecordBuilder(categoryRecord);
        KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer(registryClient);
        kafkaClient.sendMessages(
                testCategoryTopic,
                valueSerializer,
                categoryBuilder.set("category_id", 1L).set("name", "electronics").build());

        List<String> sqlLines =
                Arrays.asList(
                        "CREATE TABLE category (",
                        " category_id BIGINT,",
                        " name STRING,",
                        " description STRING", // new field, should create new schema version, but
                        // still should
                        // be able to read old version
                        ") WITH (",
                        " 'connector' = 'kafka',",
                        " 'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        " 'topic' = '" + testCategoryTopic + "',",
                        " 'scan.startup.mode' = 'earliest-offset',",
                        " 'properties.group.id' = 'test-group',",
                        " 'format' = 'avro-confluent',",
                        " 'avro-confluent.url' = 'http://"
                                + INTER_CONTAINER_REGISTRY_ALIAS
                                + ":8082'",
                        ");",
                        "",
                        "CREATE TABLE results (",
                        " category_id BIGINT,",
                        " name STRING,",
                        " description STRING",
                        ") WITH (",
                        " 'connector' = 'kafka',",
                        " 'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        " 'properties.group.id' = 'test-group',",
                        " 'topic' = '" + testResultsTopic + "',",
                        " 'format' = 'csv',",
                        " 'csv.null-literal' = 'null'",
                        ");",
                        "",
                        "INSERT INTO results SELECT * FROM category;");

        executeSqlStatements(sqlLines);
        List<String> categories =
                kafkaClient.readMessages(
                        1, "test-group", testResultsTopic, new StringDeserializer());
        assertThat(categories, equalTo(Collections.singletonList("1,electronics,null")));
    }

    @Test
    public void testWriting() throws Exception {
        String testUserBehaviorTopic = "test-user-behavior-" + UUID.randomUUID().toString();
        // Create topic test-avro
        kafkaClient.createTopic(1, 1, testUserBehaviorTopic);

        String behaviourSubject = testUserBehaviorTopic + "-value";
        List<String> sqlLines =
                Arrays.asList(
                        "CREATE TABLE user_behavior (",
                        " user_id BIGINT NOT NULL,",
                        " item_id BIGINT,",
                        " category_id BIGINT,",
                        " behavior STRING,",
                        " ts TIMESTAMP(3)",
                        ") WITH (",
                        " 'connector' = 'kafka',",
                        " 'properties.bootstrap.servers' = '"
                                + INTER_CONTAINER_KAFKA_ALIAS
                                + ":9092',",
                        " 'topic' = '" + testUserBehaviorTopic + "',",
                        " 'format' = 'avro-confluent',",
                        " 'avro-confluent.url' = 'http://"
                                + INTER_CONTAINER_REGISTRY_ALIAS
                                + ":8082"
                                + "'",
                        ");",
                        "",
                        "INSERT INTO user_behavior VALUES (1, 1, 1, 'buy', TO_TIMESTAMP(FROM_UNIXTIME(1234)));");

        executeSqlStatements(sqlLines);

        List<Integer> versions = getAllVersions(behaviourSubject);
        assertThat(versions.size(), equalTo(1));
        List<Object> userBehaviors =
                kafkaClient.readMessages(
                        1,
                        "test-group",
                        testUserBehaviorTopic,
                        new KafkaAvroDeserializer(registryClient));

        String schemaString =
                registryClient.getByVersion(behaviourSubject, versions.get(0), false).getSchema();
        Schema userBehaviorSchema = new Schema.Parser().parse(schemaString);
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(userBehaviorSchema);
        assertThat(
                userBehaviors,
                equalTo(
                        Collections.singletonList(
                                recordBuilder
                                        .set("user_id", 1L)
                                        .set("item_id", 1L)
                                        .set("category_id", 1L)
                                        .set("behavior", "buy")
                                        .set("ts", 1234000L)
                                        .build())));
    }

    private List<Integer> getAllVersions(String behaviourSubject) throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(120));
        Exception ex =
                new IllegalStateException(
                        "Could not query schema registry. Negative deadline provided.");
        while (deadline.hasTimeLeft()) {
            try {
                return registryClient.getAllVersions(behaviourSubject);
            } catch (RestClientException e) {
                ex = e;
            }
        }
        throw ex;
    }

    private void executeSqlStatements(List<String> sqlLines) throws Exception {
        flink.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(
                                sqlAvroJar, sqlAvroRegistryJar, sqlConnectorKafkaJar, sqlToolBoxJar)
                        .build());
    }
}
