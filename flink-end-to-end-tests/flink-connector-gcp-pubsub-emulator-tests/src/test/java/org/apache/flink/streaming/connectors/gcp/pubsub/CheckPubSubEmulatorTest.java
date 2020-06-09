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

import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubsubHelper;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

/**
 * Tests to ensure the docker image with PubSub is working correctly.
 */
public class CheckPubSubEmulatorTest extends GCloudUnitTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(CheckPubSubEmulatorTest.class);

	private static final String PROJECT_NAME = "Project";
	private static final String TOPIC_NAME = "Topic";
	private static final String SUBSCRIPTION_NAME = "Subscription";

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
	public void testPull() throws Exception {
		Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
		publisher
			.publish(PubsubMessage
				.newBuilder()
				.setData(ByteString.copyFromUtf8("Hello World PULL"))
				.build())
			.get();

		List<ReceivedMessage> receivedMessages = pubsubHelper.pullMessages(PROJECT_NAME, SUBSCRIPTION_NAME, 1);

		assertEquals(1, receivedMessages.size());
		assertEquals("Hello World PULL", receivedMessages.get(0).getMessage().getData().toStringUtf8());

		publisher.shutdown();
	}

	@Test
	public void testPub() throws Exception {
		List<PubsubMessage> receivedMessages = new ArrayList<>();
		Subscriber subscriber = pubsubHelper.
			subscribeToSubscription(
				PROJECT_NAME,
				SUBSCRIPTION_NAME,
				(message, consumer) -> {
					receivedMessages.add(message);
					consumer.ack();
				}
			);
		subscriber.awaitRunning(5, MINUTES);

		Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
		publisher
			.publish(PubsubMessage
				.newBuilder()
				.setData(ByteString.copyFromUtf8("Hello World"))
				.build())
			.get();

		LOG.info("Waiting a while to receive the message...");

		waitUntil(() -> receivedMessages.size() > 0);

		assertEquals(1, receivedMessages.size());
		assertEquals("Hello World", receivedMessages.get(0).getData().toStringUtf8());

		LOG.info("Received message. Shutting down ...");

		subscriber.stopAsync().awaitTerminated(5, MINUTES);
		publisher.shutdown();
	}

	/*
	 * Returns when predicate returns true or if 10 seconds have passed
	 */
	private void waitUntil(Supplier<Boolean> predicate) throws InterruptedException {
		int retries = 0;

		while (!predicate.get() && retries < 100) {
			retries++;
			Thread.sleep(10);
		}
	}

}
