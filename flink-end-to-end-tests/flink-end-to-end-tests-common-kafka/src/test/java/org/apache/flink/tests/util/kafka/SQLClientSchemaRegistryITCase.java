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
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.FlinkContainer;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.tests.util.kafka.containers.SchemaRegistryContainer;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
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
@Category(value = {TravisGroup1.class})
public class SQLClientSchemaRegistryITCase {
    public static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    public static final String INTER_CONTAINER_REGISTRY_ALIAS = "registry";
    private static final Path sqlAvroJar = TestUtils.getResource(".*avro.jar");
    private static final Path sqlAvroRegistryJar = TestUtils.getResource(".*avro-confluent.jar");
    private static final Path sqlToolBoxJar = TestUtils.getResource(".*SqlToolbox.jar");
    private final Path sqlConnectorKafkaJar = TestUtils.getResource(".*kafka.jar");

    public final Network network = Network.newNetwork();

    @ClassRule public static final Timeout TIMEOUT = new Timeout(10, TimeUnit.MINUTES);

    @Rule
    public final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"))
                    .withNetwork(network)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Rule
    public final SchemaRegistryContainer registry =
            new SchemaRegistryContainer("5.5.2")
                    .withKafka(INTER_CONTAINER_KAFKA_ALIAS + ":9092")
                    .withNetwork(network)
                    .withNetworkAliases(INTER_CONTAINER_REGISTRY_ALIAS)
                    .dependsOn(kafka);

    @Rule
    public final FlinkContainer flink =
            FlinkContainer.builder().build().withNetwork(network).dependsOn(kafka);

    private final KafkaContainerClient kafkaClient = new KafkaContainerClient(kafka);
    private CachedSchemaRegistryClient registryClient;

    @Before
    public void setUp() {
        registryClient = new CachedSchemaRegistryClient(registry.getSchemaRegistryUrl(), 10);
    }

    @Test(timeout = 120_000)
    public void testReading() throws Exception {
        String testCategoryTopic = "test-category-" + UUID.randomUUID().toString();
        String testResultsTopic = "test-results-" + UUID.randomUUID().toString();
        kafkaClient.createTopic(1, 1, testCategoryTopic);
        Schema categoryRecord =
                SchemaBuilder.record("record")
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
                        " 'format' = 'avro-confluent',",
                        " 'avro-confluent.schema-registry.url' = 'http://"
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

    @Test(timeout = 120_000)
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
                        " 'avro-confluent.schema-registry.url' = 'http://"
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

        Schema userBehaviorSchema =
                (Schema)
                        registryClient
                                .getSchemaBySubjectAndId(behaviourSubject, versions.get(0))
                                .rawSchema();
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
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
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
