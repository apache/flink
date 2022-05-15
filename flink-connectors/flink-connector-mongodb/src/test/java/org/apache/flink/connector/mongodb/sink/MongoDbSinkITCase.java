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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.util.Properties;

import static org.apache.flink.connector.mongodb.util.MongoDbConfigConstants.APPLY_CONNECTION_STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for using MongoDB Sink based on MongoDB TestContainers. */
public class MongoDbSinkITCase extends TestLogger {

    private final MongoDbWriteOperationConverter<String> mongoDbWriteOperationConverter =
            s -> new MongoDbInsertOneOperation(ImmutableMap.of("key", s));

    private final MongoDbWriteOperationConverter<String> mongoDbWriteOperationConverterDuplicateId =
            s -> new MongoDbInsertOneOperation(ImmutableMap.of("_id", "duplicate-id"));

    @ClassRule
    public static final MongoDBContainer MONGO_DB_CONTAINER =
            new MongoDBContainer(DockerImageName.parse(DockerImageVersions.MONGODB))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("mongodb");

    private StreamExecutionEnvironment env;
    private MongoClient mongoClient;

    @BeforeClass
    public static void beforeClass() {
        MONGO_DB_CONTAINER.start();
    }

    @AfterClass
    public static void afterClass() {
        MONGO_DB_CONTAINER.stop();
    }

    @Before
    public void setUp() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        mongoClient = MongoClients.create(MONGO_DB_CONTAINER.getReplicaSetUrl());
    }

    @After
    public void teardown() {
        mongoClient.close();
    }

    @Test
    public void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Exception {

        new Scenario()
                .withDatabaseName("test-database-name-1")
                .withCollectionName("test-collection-name-1")
                .runScenario();
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {

        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .withDatabaseName("test-database-name-2")
                .withCollectionName("test-collection-name-2")
                .runScenario();
    }

    @Test
    public void multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted()
            throws Exception {

        new Scenario()
                .withNumberOfElementsToSend(150)
                .withSizeOfMessageBytes(2500)
                .withBufferMaxTimeMS(2000)
                .withMaxInflightReqs(10)
                .withMaxBatchSize(20)
                .withExpectedElements(150)
                .withDatabaseName("test-database-name-3")
                .withCollectionName("test-collection-name-3")
                .runScenario();
    }

    @Test
    public void writingRecordsWithDuplicateIdShouldResultInFailureInFailOnErrorIsOn() {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withDatabaseName("test-database-name-4")
                                        .withCollectionName("test-collection-name-4")
                                        .withFailOnError(true)
                                        .withMongoDbWriteOperationConverter(
                                                mongoDbWriteOperationConverterDuplicateId)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining(
                        "Encountered an exception while persisting records, not retrying due to {failOnError} being set.");
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(APPLY_CONNECTION_STRING, MONGO_DB_CONTAINER.getReplicaSetUrl());
        return properties;
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String databaseName;
        private String collectionName = null;
        private MongoDbWriteOperationConverter<String> mongoDbWriteOperationConverter =
                MongoDbSinkITCase.this.mongoDbWriteOperationConverter;

        private Properties properties = MongoDbSinkITCase.this.getDefaultProperties();

        public void runScenario() throws Exception {
            prepareCollection(databaseName, collectionName);

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            MongoDbSink<String> mongoDbSink =
                    MongoDbSink.<String>builder()
                            .setMongoDbWriteOperationConverter(null)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setDatabaseName(databaseName)
                            .setCollectionName(collectionName)
                            .setMongoProperties(properties)
                            .setFailOnError(true)
                            .setMongoDbWriteOperationConverter(mongoDbWriteOperationConverter)
                            .build();

            stream.sinkTo(mongoDbSink);

            env.execute("MongoDB Async Sink Example Program");

            Long count =
                    Mono.from(
                                    mongoClient
                                            .getDatabase(databaseName)
                                            .getCollection(collectionName)
                                            .countDocuments())
                            .block();
            assertThat(count).isEqualTo(expectedElements);
        }

        public Scenario withNumberOfElementsToSend(int numberOfElementsToSend) {
            this.numberOfElementsToSend = numberOfElementsToSend;
            return this;
        }

        public Scenario withSizeOfMessageBytes(int sizeOfMessageBytes) {
            this.sizeOfMessageBytes = sizeOfMessageBytes;
            return this;
        }

        public Scenario withBufferMaxTimeMS(int bufferMaxTimeMS) {
            this.bufferMaxTimeMS = bufferMaxTimeMS;
            return this;
        }

        public Scenario withMaxInflightReqs(int maxInflightReqs) {
            this.maxInflightReqs = maxInflightReqs;
            return this;
        }

        public Scenario withMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Scenario withExpectedElements(int expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Scenario withFailOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public Scenario withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Scenario withCollectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Scenario withMongoDbWriteOperationConverter(
                MongoDbWriteOperationConverter<String> mongoDbWriteOperationConverter) {
            this.mongoDbWriteOperationConverter = mongoDbWriteOperationConverter;
            return this;
        }

        public Scenario withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        private void prepareCollection(String databaseName, String collectionName)
                throws Exception {
            Mono.from(mongoClient.getDatabase(databaseName).createCollection(collectionName))
                    .block();
        }
    }
}
