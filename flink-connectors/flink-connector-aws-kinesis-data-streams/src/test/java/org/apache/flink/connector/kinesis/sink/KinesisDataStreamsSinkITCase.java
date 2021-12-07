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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connectors.kinesis.testutils.KinesaliteContainer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.time.Duration;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;

/** IT cases for using Kinesis Data Streams Sink based on Kinesalite. */
public class KinesisDataStreamsSinkITCase extends TestLogger {

    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";

    private final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            KinesisDataStreamsSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                    .build();

    private final ElementConverter<String, PutRecordsRequestEntry>
            partitionKeyTooLongElementConverter =
                    KinesisDataStreamsSinkElementConverter.<String>builder()
                            .setSerializationSchema(new SimpleStringSchema())
                            .setPartitionKeyGenerator(element -> element)
                            .build();

    @ClassRule
    public static final KinesaliteContainer KINESALITE =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE))
                    .withNetwork(Network.newNetwork())
                    .withNetworkAliases("kinesalite");

    private StreamExecutionEnvironment env;
    private KinesisAsyncClient kinesisClient;

    @Before
    public void setUp() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        kinesisClient = KINESALITE.getHostClient();
    }

    @After
    public void teardown() {
        System.clearProperty(SdkSystemSetting.CBOR_ENABLED.property());
    }

    @Test
    public void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Exception {

        new Scenario()
                .withKinesaliteStreamName("test-stream-name-1")
                .withSinkConnectionStreamName("test-stream-name-1")
                .runScenario();
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {

        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .withKinesaliteStreamName("test-stream-name-2")
                .withSinkConnectionStreamName("test-stream-name-2")
                .runScenario();
    }

    @Test
    public void veryLargeMessagesSucceedInBeingPersisted() throws Exception {

        new Scenario()
                .withNumberOfElementsToSend(5)
                .withSizeOfMessageBytes(2500)
                .withMaxBatchSize(10)
                .withExpectedElements(5)
                .withKinesaliteStreamName("test-stream-name-3")
                .withSinkConnectionStreamName("test-stream-name-3")
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
                .withKinesaliteStreamName("test-stream-name-4")
                .withSinkConnectionStreamName("test-stream-name-4")
                .runScenario();
    }

    @Test
    public void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOn() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(true, "test-stream-name-5");
    }

    @Test
    public void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOff() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(false, "test-stream-name-6");
    }

    @Test
    public void veryLargeMessagesFailGracefullyWithBrokenElementConverter() {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withNumberOfElementsToSend(5)
                                        .withSizeOfMessageBytes(2500)
                                        .withExpectedElements(5)
                                        .withKinesaliteStreamName("test-stream-name-7")
                                        .withSinkConnectionStreamName("test-stream-name-7")
                                        .withElementConverter(partitionKeyTooLongElementConverter)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining(
                        "Encountered an exception while persisting records, not retrying due to {failOnError} being set.");
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String kinesaliteStreamName;
        private String sinkConnectionStreamName;
        private ElementConverter<String, PutRecordsRequestEntry> elementConverter =
                KinesisDataStreamsSinkITCase.this.elementConverter;

        public void runScenario() throws Exception {
            prepareStream(kinesaliteStreamName);

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<String>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            Properties prop = new Properties();
            prop.setProperty(AWS_ENDPOINT, KINESALITE.getHostEndpointUrl());
            prop.setProperty(AWS_ACCESS_KEY_ID, KINESALITE.getAccessKey());
            prop.setProperty(AWS_SECRET_ACCESS_KEY, KINESALITE.getSecretKey());
            prop.setProperty(AWS_REGION, KINESALITE.getRegion().toString());
            prop.setProperty(TRUST_ALL_CERTIFICATES, "true");
            prop.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

            KinesisDataStreamsSink<String> kdsSink =
                    KinesisDataStreamsSink.<String>builder()
                            .setElementConverter(elementConverter)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setStreamName(sinkConnectionStreamName)
                            .setKinesisClientProperties(prop)
                            .setFailOnError(true)
                            .build();

            stream.sinkTo(kdsSink);

            env.execute("KDS Async Sink Example Program");

            String shardIterator =
                    kinesisClient
                            .getShardIterator(
                                    GetShardIteratorRequest.builder()
                                            .shardId(DEFAULT_FIRST_SHARD_NAME)
                                            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                            .streamName(kinesaliteStreamName)
                                            .build())
                            .get()
                            .shardIterator();

            Assertions.assertThat(
                            kinesisClient
                                    .getRecords(
                                            GetRecordsRequest.builder()
                                                    .shardIterator(shardIterator)
                                                    .build())
                                    .get()
                                    .records()
                                    .size())
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

        public Scenario withSinkConnectionStreamName(String sinkConnectionStreamName) {
            this.sinkConnectionStreamName = sinkConnectionStreamName;
            return this;
        }

        public Scenario withKinesaliteStreamName(String kinesaliteStreamName) {
            this.kinesaliteStreamName = kinesaliteStreamName;
            return this;
        }

        public Scenario withElementConverter(
                ElementConverter<String, PutRecordsRequestEntry> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }

        private void prepareStream(String streamName) throws Exception {
            final RateLimiter rateLimiter =
                    RateLimiterBuilder.newBuilder()
                            .withRate(1, SECONDS)
                            .withConstantThroughput()
                            .build();

            KinesisAsyncClient kinesisClient = KINESALITE.getHostClient();
            kinesisClient
                    .createStream(
                            CreateStreamRequest.builder()
                                    .streamName(streamName)
                                    .shardCount(1)
                                    .build())
                    .get();

            Deadline deadline = Deadline.fromNow(Duration.ofMinutes(1));
            while (!rateLimiter.getWhenReady(() -> streamExists(streamName))) {
                if (deadline.isOverdue()) {
                    throw new RuntimeException("Failed to create stream within time");
                }
            }
        }

        private boolean streamExists(final String streamName) {
            try {
                return kinesisClient
                                .describeStream(
                                        DescribeStreamRequest.builder()
                                                .streamName(streamName)
                                                .build())
                                .get()
                                .streamDescription()
                                .streamStatus()
                        == StreamStatus.ACTIVE;
            } catch (Exception e) {
                return false;
            }
        }
    }

    private void testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(
            boolean failOnError, String streamName) {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withKinesaliteStreamName(streamName)
                                        .withSinkConnectionStreamName("non-existent-stream")
                                        .withFailOnError(failOnError)
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining("Encountered non-recoverable exception");
    }
}
