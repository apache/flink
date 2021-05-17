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

package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubSubSubscriberFactoryForEmulator;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubsubHelper;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.gcp.pubsub.SimpleStringSchemaWithStopMarkerDetection.STOP_MARKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test of the PubSub SOURCE with the Google PubSub emulator. */
public class EmulatedPubSubSourceTest extends GCloudUnitTestBase {
    private static final String PROJECT_NAME = "FLProject";
    private static final String TOPIC_NAME = "FLTopic";
    private static final String SUBSCRIPTION_NAME = "FLSubscription";

    private static PubsubHelper pubsubHelper;

    @BeforeClass
    public static void setUp() throws Exception {
        pubsubHelper = getPubsubHelper();
        pubsubHelper.createTopic(PROJECT_NAME, TOPIC_NAME);
        pubsubHelper.createSubscription(PROJECT_NAME, SUBSCRIPTION_NAME, PROJECT_NAME, TOPIC_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
        pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);
    }

    // IMPORTANT: This test makes use of things that happen in the emulated PubSub that
    //            are GUARANTEED to be different in the real Google hosted PubSub.
    //            So running these tests against the real thing will have a very high probability of
    // failing.
    // The assumptions:
    // 1) The ordering of the messages is maintained.
    //    We are inserting a STOP_MARKER _after_ the set of test measurements and we assume this
    // STOP event will
    //    arrive after the actual test data so we can stop the processing. In the real PubSub this
    // is NOT true.
    // 2) Exactly once: We assume that every message we put in comes out exactly once.
    //    In the real PubSub there are a lot of situations (mostly failure/retry) where this is not
    // true.
    @Test
    public void testFlinkSource() throws Exception {
        // Create some messages and put them into pubsub
        List<String> input =
                Arrays.asList(
                        "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
                        "Ten");

        List<String> messagesToSend = new ArrayList<>(input);

        // Now add some stream termination messages.
        // NOTE: Messages are pulled from PubSub in batches by the source.
        //       So we need enough STOP_MARKERs to ensure ALL parallel tasks get at least one
        // STOP_MARKER
        //       If not then at least one task will not terminate and the test will not end.
        //       We pull 3 at a time, have 4 parallel: We need at least 12 STOP_MARKERS
        IntStream.rangeClosed(1, 20).forEach(i -> messagesToSend.add(STOP_MARKER));

        // IMPORTANT NOTE: This way of testing uses an effect of the PubSub emulator that is
        // absolutely
        // guaranteed NOT to work in the real PubSub: The ordering of the messages is maintained in
        // the topic.
        // So here we can assume that if we add a stop message LAST we can terminate the test stream
        // when we see it.

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
        env.setParallelism(4);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<String> fromPubSub =
                env.addSource(
                                PubSubSource.newBuilder()
                                        .withDeserializationSchema(
                                                new SimpleStringSchemaWithStopMarkerDetection())
                                        .withProjectName(PROJECT_NAME)
                                        .withSubscriptionName(SUBSCRIPTION_NAME)
                                        .withCredentials(EmulatorCredentials.getInstance())
                                        .withPubSubSubscriberFactory(
                                                new PubSubSubscriberFactoryForEmulator(
                                                        getPubSubHostPort(),
                                                        PROJECT_NAME,
                                                        SUBSCRIPTION_NAME,
                                                        10,
                                                        Duration.ofSeconds(1),
                                                        3))
                                        .build())
                        .name("PubSub source");

        List<String> output = new ArrayList<>();
        DataStreamUtils.collect(fromPubSub).forEachRemaining(output::add);

        assertEquals("Wrong number of elements", input.size(), output.size());
        for (String test : input) {
            assertTrue("Missing " + test, output.contains(test));
        }
    }
}
