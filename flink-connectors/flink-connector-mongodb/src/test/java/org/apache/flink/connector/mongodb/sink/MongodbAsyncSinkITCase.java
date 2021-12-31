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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.TestLogger;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.connector.mongodb.common.MongodbConfigConstants.APPLY_CONNECTION_STRING;

/** IT cases for using Mongodb Async Sink. */
public class MongodbAsyncSinkITCase extends TestLogger {

    private static final ElementConverter<String, Document> elementConverter =
            MongodbAsyncSinkElementConverter.<String>builder()
                    .setSerializationSchema(
                            new DocumentSerializer<String>() {
                                @Override
                                public Document serialize(String str) {
                                    Document document = new Document();
                                    return document.append("_id", str);
                                }
                            })
                    .build();

    private MongodExecutable mongodExe;
    private MongodProcess mongod;
    private MongoClient mongo;
    // these can be overridden by subclasses
    private static final String MONGODB_HOST = "127.0.0.1";
    private static final int MONGODB_PORT = 27018;
    private static final String DATABASE_NAME = "testdb";
    private static final String COLLECTION = "testcoll";
    private static final String CONNECT_STRING =
            String.format("mongodb://%s:%d/%s", MONGODB_HOST, MONGODB_PORT, DATABASE_NAME);

    private StreamExecutionEnvironment env;

    @Before
    public void setUp() throws IOException {
        MongodStarter starter = MongodStarter.getDefaultInstance();
        MongodConfig mongodConfig =
                MongodConfig.builder()
                        .version(Version.Main.V4_0)
                        .net(new Net(MONGODB_HOST, MONGODB_PORT, Network.localhostIsIPv6()))
                        .build();
        this.mongodExe = starter.prepare(mongodConfig);
        this.mongod = mongodExe.start();
        this.mongo = MongoClients.create(CONNECT_STRING);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    @After
    public void after() {
        if (this.mongo != null) {
            mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).drop();
            mongo.close();
        }
        if (this.mongod != null) {
            this.mongod.stop();
            this.mongodExe.stop();
        }
    }

    @Test
    public void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Exception {
        new Scenario().runScenario();
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {

        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .runScenario();
    }

    @Test
    public void veryLargeMessagesSucceedInBeingPersisted() throws Exception {

        new Scenario()
                .withNumberOfElementsToSend(5)
                .withSizeOfMessageBytes(2500)
                .withMaxBatchSize(10)
                .withExpectedElements(5)
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
                .runScenario();
    }

    @Test
    public void repeatkeyShouldResultInFailureInFailOnErrorIsOn() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(true);
    }

    @Test
    public void repeatkeyShouldResultInFailureInFailOnErrorIsOff() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(false);
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private String database = DATABASE_NAME;
        private boolean failOnError = false;
        private ElementConverter<String, Document> elementConverter =
                MongodbAsyncSinkITCase.this.elementConverter;

        public void runScenario() throws Exception {

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            Properties prop = new Properties();
            prop.setProperty(APPLY_CONNECTION_STRING, CONNECT_STRING);

            MongodbAsyncSink<String> mongodbSink =
                    MongodbAsyncSink.<String>builder()
                            .setElementConverter(elementConverter)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setDatabase(database)
                            .setCollection(COLLECTION)
                            .setMongodbClientProperties(prop)
                            .build();

            stream.sinkTo(mongodbSink);

            env.execute("Mongodb Async Sink Example Program");

            Mono.from(mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).countDocuments())
                    .doFinally(
                            r -> {
                                Assertions.assertThat(r).isEqualTo(expectedElements);
                            })
                    .subscribe();
        }

        public void runScenarioTwo() throws Exception {

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<>(
                                            new DuplicatePrimaryKeyDataGenerator(),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            Properties prop = new Properties();
            prop.setProperty(APPLY_CONNECTION_STRING, CONNECT_STRING);

            MongodbAsyncSink<String> mongodbSink =
                    MongodbAsyncSink.<String>builder()
                            .setElementConverter(elementConverter)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setDatabase(database)
                            .setCollection(COLLECTION)
                            .setMongodbClientProperties(prop)
                            .build();

            stream.sinkTo(mongodbSink);

            env.execute("Mongodb Async Sink Example Program");

            Mono.from(mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).countDocuments())
                    .doFinally(
                            r -> {
                                Assertions.assertThat(r).isEqualTo(expectedElements);
                            })
                    .subscribe();
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

        public Scenario withDatabase(String database) {
            this.database = database;
            return this;
        }
    }

    class DuplicatePrimaryKeyDataGenerator implements DataGenerator<String> {

        private int count;

        @Override
        public void open(
                String s,
                FunctionInitializationContext functionInitializationContext,
                RuntimeContext runtimeContext)
                throws Exception {}

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            return new Random().nextInt(2) + "";
        }
    }

    private void testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(
            boolean failOnError) {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(() -> new Scenario().withFailOnError(failOnError).runScenarioTwo())
                .havingCause()
                .havingCause()
                .withMessageContaining("Encountered non-recoverable exception");
    }
}
