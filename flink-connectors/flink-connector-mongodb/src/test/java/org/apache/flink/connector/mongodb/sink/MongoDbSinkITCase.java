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

import org.apache.flink.connector.mongodb.config.MongoConfigConstants;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import com.mongodb.MongoTimeoutException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** IT cases for using MongoDb Sink based on TestContainers. */
public class MongoDbSinkITCase extends TestLogger {

    private static final String TEST_DB = "test_db";

    @Rule
    public MongoDBContainer mongoDBContainer =
            new MongoDBContainer(DockerImageName.parse("mongo:4.4.12"));

    private final DocumentGenerator<String> documentGenerator =
            string -> ImmutableMap.of("_id", "id" + string, "key", string);

    private final FilterGenerator<String> filterGenerator =
            string -> ImmutableMap.of("_id", "id" + string);

    private StreamExecutionEnvironment env;
    private MongoClient mongoClient;

    @Before
    public void before() throws Throwable {
        mongoDBContainer.start();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        mongoClient = MongoClients.create(mongoDBContainer.getReplicaSetUrl(TEST_DB));
    }

    @After
    public void after() {
        mongoClient.close();
    }

    @Test
    public void elementsWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Throwable {
        new Scenario()
                .withMongoCollectionName("test_collection_1")
                .withMaxBatchSize(1)
                .runScenario();
    }

    @Test
    public void multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted()
            throws Throwable {
        new Scenario()
                .withNumberOfElementsToSend(150)
                .withSizeOfMessageBytes(2500)
                .withBufferMaxTimeMS(2000)
                .withMaxInflightReqs(10)
                .withMaxBatchSize(20)
                .withExpectedElements(150)
                .withMongoCollectionName("test_collection_2")
                .runScenario();
    }

    @Test
    public void invalidReplaceRequestShouldResultInFailureInFailOnErrorIsOn() {
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(
                        () ->
                                new Scenario()
                                        .withMongoCollectionName("test_collection_3")
                                        .withFailOnError(true)
                                        // not filtering on _id should fail the replacement
                                        .withFilterGenerator(s -> ImmutableMap.of())
                                        .runScenario())
                .havingCause()
                .havingCause()
                .withMessageContaining(
                        "Encountered an exception while persisting records, not retrying due to {failOnError} being set.");
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Throwable {

        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .withMongoCollectionName("test_collection_4")
                .runScenario();
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String mongoCollectionName;
        private DocumentGenerator<String> documentGenerator =
                MongoDbSinkITCase.this.documentGenerator;
        private FilterGenerator<String> filterGenerator = MongoDbSinkITCase.this.filterGenerator;

        public void runScenario() throws Throwable {
            prepareCollection(TEST_DB, mongoCollectionName);

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<String>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            Properties prop = new Properties();
            prop.put(
                    MongoConfigConstants.CONNECTION_STRING,
                    mongoDBContainer.getReplicaSetUrl(TEST_DB));

            MongoDbSink<String> mongoDbSink =
                    MongoDbSink.<String>builder()
                            .setDocumentGenerator(documentGenerator)
                            .setFilterGenerator(filterGenerator)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setDatabaseName(TEST_DB)
                            .setCollectionName(mongoCollectionName)
                            .setMongoClientProperties(prop)
                            .setFailOnError(true)
                            .build();

            stream.sinkTo(mongoDbSink);

            env.execute("MongoDb Async Sink Example Program");

            Publisher<Long> documentFindPublisher =
                    mongoClient
                            .getDatabase(TEST_DB)
                            .getCollection(mongoCollectionName)
                            .countDocuments();
            final ObservableSubscriber<Long> subscriber = new ObservableSubscriber<>();
            documentFindPublisher.subscribe(subscriber);
            subscriber.await();
            List<Long> results = subscriber.getResults();
            assertThat(results.get(0)).isEqualTo(expectedElements);
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

        public Scenario withMongoCollectionName(String mongoCollectionName) {
            this.mongoCollectionName = mongoCollectionName;
            return this;
        }

        public Scenario withFilterGenerator(FilterGenerator<String> filterGenerator) {
            this.filterGenerator = filterGenerator;
            return this;
        }

        private void prepareCollection(String databaseName, String collectionName)
                throws Exception {
            Publisher<Void> collection =
                    mongoClient.getDatabase(databaseName).createCollection(collectionName);
            final ObservableSubscriber<Void> subscriber = new ObservableSubscriber<>();
            collection.subscribe(subscriber);
            try {
                subscriber.await();
            } catch (Throwable e) {
                throw new RuntimeException("Failed to create collection");
            }
        }
    }

    private static class ObservableSubscriber<T> implements Subscriber<T> {
        private final CountDownLatch latch;
        private final List<T> results = new ArrayList<T>();

        private volatile int minimumNumberOfResults;
        private volatile int counter;
        private volatile Subscription subscription;
        private volatile Throwable error;

        public ObservableSubscriber() {
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
            subscription.request(Integer.MAX_VALUE);
        }

        @Override
        public void onNext(final T t) {
            results.add(t);
            counter++;
            if (counter >= minimumNumberOfResults) {
                latch.countDown();
            }
        }

        @Override
        public void onError(final Throwable t) {
            error = t;
            onComplete();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }

        public List<T> getResults() {
            return results;
        }

        public void await() throws Throwable {
            if (!latch.await(10, SECONDS)) {
                throw new MongoTimeoutException("Publisher timed out");
            }
            if (error != null) {
                throw error;
            }
        }

        public void waitForThenCancel(final int minimumNumberOfResults) throws Throwable {
            this.minimumNumberOfResults = minimumNumberOfResults;
            if (minimumNumberOfResults > counter) {
                await();
            }
            subscription.cancel();
        }
    }
}
