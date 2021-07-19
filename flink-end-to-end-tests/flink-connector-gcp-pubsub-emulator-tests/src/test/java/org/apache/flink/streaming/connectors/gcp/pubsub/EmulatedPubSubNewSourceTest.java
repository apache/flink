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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubSubSubscriberFactoryForEmulator;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubsubHelper;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.PubSubSource;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test of {@link PubSubSource} against the GCP Pub/Sub emulator SDK. */
public class EmulatedPubSubNewSourceTest extends GCloudUnitTestBase {
    private static final String PROJECT_NAME = "FLProject";
    private static final String TOPIC_NAME = "FLTopic";
    private static final String SUBSCRIPTION_NAME = "FLSubscription";

    private static PubsubHelper pubsubHelper;

    @Before
    public void setUp() throws Exception {
        pubsubHelper = getPubsubHelper();
        pubsubHelper.createTopic(PROJECT_NAME, TOPIC_NAME);
        pubsubHelper.createSubscription(PROJECT_NAME, SUBSCRIPTION_NAME, PROJECT_NAME, TOPIC_NAME);
    }

    @After
    public void tearDown() throws Exception {
        pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
        pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);
    }

    public void testFlinkSource(boolean testWithFailure) throws Exception {
        // Create some messages and put them into pubsub
        List<String> input =
                Arrays.asList(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten");

        List<String> messagesToSend = new ArrayList<>(input);

        // Publish the messages into PubSub
        Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
        messagesToSend.forEach(
                s -> {
                    try {
                        publisher
                                .publish(
                                        PubsubMessage.newBuilder()
                                                .setData(ByteString.copyFromUtf8(s))
                                                .build())
                                .get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setParallelism(1);
        if (testWithFailure) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));
        } else {
            env.setRestartStrategy(RestartStrategies.noRestart());
        }

        PubSubSource<String> source =
                PubSubSource.<String>builder()
                        .setDeserializationSchema(new SimpleStringSchema())
                        .setProjectName(PROJECT_NAME)
                        .setSubscriptionName(SUBSCRIPTION_NAME)
                        .setCredentials(EmulatorCredentials.getInstance())
                        .setPubSubSubscriberFactory(
                                new PubSubSubscriberFactoryForEmulator(
                                        getPubSubHostPort(),
                                        PROJECT_NAME,
                                        SUBSCRIPTION_NAME,
                                        10,
                                        Duration.ofSeconds(1),
                                        3))
                        .build();

        DataStream<String> fromPubSub =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "test-pubsub-new-source");

        if (testWithFailure) {
            fromPubSub = fromPubSub.map(new FailureMapFunction<>(3));
        }

        List<String> output = new ArrayList<>();
        DataStreamUtils.collect(fromPubSub).forEachRemaining(output::add);

        assertEquals("Wrong number of elements", input.size(), output.size());
        for (String test : input) {
            assertTrue("Missing " + test, output.contains(test));
        }
    }

    private class FailureMapFunction<T> extends RichMapFunction<T, T> {
        private final long numberOfRecordsUntilFailure;
        private long numberOfRecordsProcessed;

        private FailureMapFunction(long numberOfRecordsBeforeFailure) {
            this.numberOfRecordsUntilFailure = numberOfRecordsBeforeFailure;
        }

        @Override
        public T map(T value) throws Exception {
            numberOfRecordsProcessed++;

            if (shouldThrowException()) {
                throw new Exception(
                        "Deliberately thrown exception to induce crash for failure recovery testing.");
            }
            return value;
        }

        private boolean shouldThrowException() {
            return getRuntimeContext().getAttemptNumber() <= 1
                    && (numberOfRecordsProcessed >= numberOfRecordsUntilFailure);
        }
    }

    // IMPORTANT: This test makes use of things that happen in the emulated PubSub that
    //            are GUARANTEED to be different in the real Google hosted PubSub.
    //            So running these tests against the real thing will have a very high probability of
    // failing.
    // The assumptions:
    // 1) The ordering of the messages is maintained.
    // 2) Exactly once: We assume that every message we put in comes out exactly once.
    //    In the real PubSub there are a lot of situations (mostly failure/retry) where this is not
    // true.
    @Test
    public void testFlinkSourceOk() throws Exception {
        testFlinkSource(false);
    }

    @Test
    public void testFlinkSourceFailure() throws Exception {
        testFlinkSource(true);
    }
}
