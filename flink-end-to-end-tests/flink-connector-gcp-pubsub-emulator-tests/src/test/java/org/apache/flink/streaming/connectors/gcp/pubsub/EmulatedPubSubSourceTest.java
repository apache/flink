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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubsubHelper;

import com.google.cloud.NoCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test of the PubSub SOURCE with the Google PubSub emulator.
 */
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

	@Test
	public void testFlinkSource() throws Exception {
		// Create some messages and put them into pubsub
		List<String> input = Arrays.asList("One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten");

		List<String> messagesToSend = new ArrayList<>(input);
		messagesToSend.add("End");

		// Publish the messages into PubSub
		Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
		messagesToSend.forEach(s -> {
			try {
				publisher
					.publish(PubsubMessage.newBuilder()
						.setData(ByteString.copyFromUtf8(s))
						.build())
					.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(1000);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<String> fromPubSub = env
			.addSource(PubSubSource.newBuilder()
								.withDeserializationSchema(new BoundedStringDeserializer(10))
								.withProjectName(PROJECT_NAME)
								.withSubscriptionName(SUBSCRIPTION_NAME)
								.withCredentials(NoCredentials.getInstance())
								.withPubSubSubscriberFactory(new PubSubSubscriberFactoryForEmulator(getPubSubHostPort(), PROJECT_NAME, SUBSCRIPTION_NAME, 10, Duration.ofSeconds(15), 100))
								.build())
			.name("PubSub source");

		List<String> output = new ArrayList<>();
		fromPubSub.writeUsingOutputFormat(new LocalCollectionOutputFormat<>(output));

		env.execute();

		assertEquals("Wrong number of elements", input.size(), output.size());
		for (String test : input) {
			assertTrue("Missing " + test, output.contains(test));
		}
	}

	private static class BoundedStringDeserializer implements PubSubDeserializationSchema<String> {
		private final int maxMessage;
		private int counter;

		private BoundedStringDeserializer(int maxMessages) {
			this.maxMessage = maxMessages;
			this.counter = 0;
		}

		@Override
		public boolean isEndOfStream(String message) {
			counter++;
			return counter > maxMessage;
		}

		@Override
		public String deserialize(PubsubMessage message) throws Exception {
			return message.getData().toString(StandardCharsets.UTF_8);
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return Types.STRING;
		}
	}
}
