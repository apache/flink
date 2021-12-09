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

package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.mongodb.streaming.serde.DocumentSerializer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.TestLogger;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** IT cases for using Mongodb Sink based on xx. */
public class MongodbAsyncSinkITCase extends TestLogger {
    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";

    private final ElementConverter<String, Document> elementConverter =
            MongodbAsyncSinkElementConverter.<String>builder()
                    .setSerializationSchema(
                            new DocumentSerializer<String>() {
                                @Override
                                public Document serialize(String str) {
                                    return new Document().append("_id", str);
                                }
                            })
                    .build();

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() throws Exception {

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    @Test
    public void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Exception {
        log("Running elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached");

        new Scenario()
                .withSinkDatabase("")
                .withSinkCollection("")
                .withSinkConnectionString("")
                .runScenario();
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {
        log(
                "Running elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive");

        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .withSinkDatabase("")
                .withSinkCollection("")
                .withSinkConnectionString("")
                .runScenario();
    }

    @Test
    public void multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted()
            throws Exception {
        log("Running multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted");

        new Scenario()
                .withNumberOfElementsToSend(150)
                .withSizeOfMessageBytes(2500)
                .withBufferMaxTimeMS(2000)
                .withMaxInflightReqs(10)
                .withMaxBatchSize(20)
                .withExpectedElements(150)
                .withSinkDatabase("")
                .withSinkCollection("")
                .withSinkConnectionString("")
                .runScenario();
    }

    @Test
    public void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOn() {
        log("Running nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOn");
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(true, "test-stream-name-5");
    }

    @Test
    public void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOff() {
        log("Running nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOff");
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(false, "test-stream-name-6");
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String collection;
        private String database;
        private String connectionString;
        private ElementConverter<String, Document> elementConverter =
                MongodbAsyncSinkITCase.this.elementConverter;

        public void runScenario() throws Exception {

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<String>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            MongodbAsyncSink<String> mongodbSink =
                    MongodbAsyncSink.<String>builder()
                            .setElementConverter(elementConverter)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setFailOnError(true)
                            .setCollection("")
                            .setDatabase("")
                            .setConnectionString("")
                            .build();

            stream.sinkTo(mongodbSink);

            env.execute("KDS Async Sink Example Program");
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

        public Scenario withSinkDatabase(String Database) {
            this.database = database;
            return this;
        }

        public Scenario withSinkConnectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Scenario withSinkCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Scenario withElementConverter(ElementConverter<String, Document> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }
    }

    private void testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(
            boolean failOnError, String streamName) {
        Throwable thrown =
                assertThrows(
                        JobExecutionException.class,
                        () ->
                                new Scenario()
                                        .withSinkCollection(streamName)
                                        .withSinkDatabase("")
                                        .withSinkConnectionString("")
                                        .withFailOnError(failOnError)
                                        .runScenario());
        assertEquals(
                "Encountered non-recoverable exception", thrown.getCause().getCause().getMessage());
    }

    private void log(String message) {
        System.out.println("out - " + message);
        System.err.println("err - " + message);
    }
}
