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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.connectors.dynamodb.config.DynamoDbTablesConfig;
import org.apache.flink.streaming.connectors.dynamodb.testutils.DynamoDBHelpers;
import org.apache.flink.streaming.connectors.dynamodb.testutils.DynamoDbContainer;
import org.apache.flink.streaming.connectors.dynamodb.util.TestDynamoDbElementConverter;
import org.apache.flink.streaming.connectors.dynamodb.util.TestMapper;
import org.apache.flink.util.DockerImageVersions;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/** Integration test for {@link DynamoDbSink}. */
public class DynamoDbSinkITCase {
    private static final String PARTITION_KEY = "key";
    private static final String SORT_KEY = "sort_key";
    private static DynamoDBHelpers dynamoDBHelpers;
    private static String testTableName;
    private static StreamExecutionEnvironment env;

    @ClassRule
    public static final DynamoDbContainer LOCALSTACK =
            new DynamoDbContainer(DockerImageName.parse(DockerImageVersions.DYNAMODB))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("dynamodb");

    @BeforeClass
    public static void init() throws Exception {
        dynamoDBHelpers = new DynamoDBHelpers(LOCALSTACK.getHostClient());
    }

    @Before
    public void setup() {
        testTableName = UUID.randomUUID().toString();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(2);
    }

    @Test
    public void testRandomDataSuccessfullyWritten() throws Exception {
        new Scenario().withTableName(testTableName).runScenario();
    }

    @Test
    public void nonExistentTableNameShouldResultInFailureWhenFailOnErrorIsTrue() throws Exception {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withTableName("NonExistentTableName")
                                        .withFailOnError(true)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining("Encountered non-recoverable exception");
    }

    @Test
    public void nonExistentTableNameShouldResultInFailureWhenFailOnErrorIsFalse() throws Exception {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withTableName("NonExistentTableName")
                                        .withFailOnError(false)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining("Encountered non-recoverable exception");
    }

    @Test
    public void veryLargeMessagesFailsSinkMaxMessageSizeCheck() throws Exception {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withNumberOfElementsToSend(5)
                                        .withSizeOfMessageBytes(500 * 1000)
                                        .withExpectedElements(5)
                                        .withTableName(testTableName)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining("The request entry sent to the buffer was of size");
    }

    @Test
    public void veryLargeMessagesFailsGracefullyWhenRejectedByDynamoDb() throws Exception {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withNumberOfElementsToSend(5)
                                        .withSizeOfMessageBytes(500 * 1000)
                                        .withMaxRecordSizeInBytes(500 * 1000 * 1000)
                                        .withMaxBatchSizeInBytes(
                                                600 * 1000 * 1000) // more than max record size
                                        .withExpectedElements(5)
                                        .withTableName(testTableName)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining("Encountered non-recoverable exception");
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String tableName;
        private DynamoDbTablesConfig tablesConfig = new DynamoDbTablesConfig();
        private long maxRecordSizeInBytes = 400 * 1000;
        private long maxBatchSizeInBytes = 16 * 1000 * 1000;

        public void runScenario() throws Exception {
            dynamoDBHelpers.createTable(testTableName, PARTITION_KEY, SORT_KEY);
            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<String>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            Properties prop = new Properties();
            prop.setProperty(AWS_ENDPOINT, LOCALSTACK.getHostEndpointUrl());
            prop.setProperty(AWS_ACCESS_KEY_ID, LOCALSTACK.getAccessKey());
            prop.setProperty(AWS_SECRET_ACCESS_KEY, LOCALSTACK.getSecretKey());
            prop.setProperty(AWS_REGION, LOCALSTACK.getRegion().toString());
            prop.setProperty(TRUST_ALL_CERTIFICATES, "true");
            prop.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

            DynamoDbSink<Map<String, AttributeValue>> dynamoDbSink =
                    DynamoDbSink.<Map<String, AttributeValue>>builder()
                            .setElementConverter(new TestDynamoDbElementConverter(tableName))
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setDynamoDbTablesConfig(tablesConfig)
                            .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                            .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                            .setDynamoDbProperties(prop)
                            .build();

            stream.map(new TestMapper(PARTITION_KEY, SORT_KEY)).sinkTo(dynamoDbSink);

            env.execute("DynamoDbSink Async Sink Example Program");

            Assertions.assertThat(dynamoDBHelpers.getItemsCount(tableName))
                    .isEqualTo(expectedElements);
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

        public Scenario withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Scenario withMaxRecordSizeInBytes(long maxRecordSizeInBytes) {
            this.maxRecordSizeInBytes = maxRecordSizeInBytes;
            return this;
        }

        public Scenario withMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            return this;
        }
    }
}
