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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubSubSubscriberFactoryForEmulator;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubsubHelper;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.gcp.pubsub.SimpleStringSchemaWithStopMarkerDetection.STOP_MARKER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This is a test using the emulator for a full topology that uses PubSub as both input and output.
 */
public class EmulatedFullTopologyTest extends GCloudUnitTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(EmulatedFullTopologyTest.class);

	private static final String PROJECT_NAME             = "Project";
	private static final String INPUT_TOPIC_NAME         = "InputTopic";
	private static final String INPUT_SUBSCRIPTION_NAME  = "InputSubscription";
	private static final String OUTPUT_TOPIC_NAME        = "OutputTopic";
	private static final String OUTPUT_SUBSCRIPTION_NAME = "OutputSubscription";

	private static PubsubHelper pubsubHelper;

	// ======================================================================================================

	@BeforeClass
	public static void setUp() throws Exception {
		pubsubHelper = getPubsubHelper();
		assertNotNull("Missing pubsubHelper.", pubsubHelper);
		pubsubHelper.createTopic(PROJECT_NAME, INPUT_TOPIC_NAME);
		pubsubHelper.createSubscription(PROJECT_NAME, INPUT_SUBSCRIPTION_NAME, PROJECT_NAME, INPUT_TOPIC_NAME);
		pubsubHelper.createTopic(PROJECT_NAME, OUTPUT_TOPIC_NAME);
		pubsubHelper.createSubscription(PROJECT_NAME, OUTPUT_SUBSCRIPTION_NAME, PROJECT_NAME, OUTPUT_TOPIC_NAME);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		assertNotNull("Missing pubsubHelper.", pubsubHelper);
		pubsubHelper.deleteSubscription(PROJECT_NAME, INPUT_SUBSCRIPTION_NAME);
		pubsubHelper.deleteTopic(PROJECT_NAME, INPUT_TOPIC_NAME);
		pubsubHelper.deleteSubscription(PROJECT_NAME, OUTPUT_SUBSCRIPTION_NAME);
		pubsubHelper.deleteTopic(PROJECT_NAME, OUTPUT_TOPIC_NAME);
	}

	// ======================================================================================================

	// IMPORTANT: This test makes use of things that happen in the emulated PubSub that
	//            are GUARANTEED to be different in the real Google hosted PubSub.
	//            So running these tests against the real thing will have a very high probability of failing.
	// The assumptions:
	// 1) The ordering of the messages is maintained.
	//    We are inserting a STOP_MARKER _after_ the set of test measurements and we assume this STOP event will
	//    arrive after the actual test data so we can stop the processing. In the real PubSub this is NOT true.
	// 2) Exactly once: We assume that every message we put in comes out exactly once.
	//    In the real PubSub there are a lot of situations (mostly failure/retry) where this is not true.
	@Test
	public void testFullTopology() throws Exception {
		// ===============================================================================
		// Step 0: The test data
		List<String> input = new ArrayList<>(Arrays.asList("One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten"));

		List<String> messagesToSend = new ArrayList<>(input);

		// Now add some stream termination messages.
		// NOTE: Messages are pulled from PubSub in batches by the source.
		//       So we need enough STOP_MARKERs to ensure ALL parallel tasks get at least one STOP_MARKER
		//       If not then at least one task will not terminate and the test will not end.
		//       We pull 3 at a time, have 4 parallel: We need at least 12 STOP_MARKERS
		IntStream.rangeClosed(1, 20).forEach(i -> messagesToSend.add(STOP_MARKER));

		// IMPORTANT NOTE: This way of testing uses an effect of the PubSub emulator that is absolutely
		// guaranteed NOT to work in the real PubSub: The ordering of the messages is maintained in the topic.
		// So here we can assume that if we add a stop message LAST we can terminate the test stream when we see it.

		// ===============================================================================
		// Step 1: We put test data into the topic
		// Publish the test messages into the input topic
		Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, INPUT_TOPIC_NAME);
		for (String s : messagesToSend) {
			publisher
				.publish(
					PubsubMessage
						.newBuilder()
						.setData(ByteString.copyFromUtf8(s))
						.build()
				).get();
		}
		publisher.shutdown();

		// ===============================================================================
		// Step 2: Now we run our topology
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(100);
		env.setParallelism(4);
		env.setRestartStrategy(RestartStrategies.noRestart());

		// Silly topology
		env
			// Read the records from PubSub
			.addSource(
				// We create a PubSubSource which is the same as the normal source but with a self termination feature.
				PubSubSource
					.newBuilder()
					.withDeserializationSchema(new SimpleStringSchemaWithStopMarkerDetection())

					// First we set the exact same settings as we would normally use.
					.withProjectName(PROJECT_NAME)
					.withSubscriptionName(INPUT_SUBSCRIPTION_NAME)

					// We use the credentials for the emulator
					.withCredentials(EmulatorCredentials.getInstance())

					// Connect to the emulator
					.withPubSubSubscriberFactory(
						new PubSubSubscriberFactoryForEmulator(
							getPubSubHostPort(),
							PROJECT_NAME,
							INPUT_SUBSCRIPTION_NAME,
							1,
							Duration.ofSeconds(1),
							3))

					// Make sure we stop the source after a timeout to cleanly end the test.
					.build())

			// The actual application: Reverse the strings
			.map((MapFunction<String, String>) StringUtils::reverse)

			// And write them back to pubsub.
			.addSink(PubSubSink
				.newBuilder()
				.withSerializationSchema(new SimpleStringSchema())

				// First we set the exact same settings as we would normally use.
				.withProjectName(PROJECT_NAME)
				.withTopicName(OUTPUT_TOPIC_NAME)

				// We use the credentials for the emulator
				.withCredentials(EmulatorCredentials.getInstance())

				// Connect to the emulator
				.withHostAndPortForEmulator(getPubSubHostPort())
				.build());

		env.execute("Running unit test");

		// ===============================================================================
		// Now we should have all the resulting data in the output topic.
		// Step 3: Get the result from the output topic and verify if everything is there
		List<ReceivedMessage> receivedMessages = pubsubHelper.pullMessages(PROJECT_NAME, OUTPUT_SUBSCRIPTION_NAME, 100);

		assertEquals("Wrong number of elements", input.size(), receivedMessages.size());

		// Check output strings
		List<String> output = new ArrayList<>();

		// Extract the actual Strings from the ReceivedMessages
		receivedMessages.forEach(msg -> output.add(msg.getMessage().getData().toStringUtf8()));

		for (String test : input) {
			String reversedTest = org.apache.commons.lang3.StringUtils.reverse(test);
			LOG.info("Checking if \"{}\" --> \"{}\" exists", test, reversedTest);
			assertTrue("Missing " + test, output.contains(reversedTest));
		}
		// ===============================================================================
	}
}
