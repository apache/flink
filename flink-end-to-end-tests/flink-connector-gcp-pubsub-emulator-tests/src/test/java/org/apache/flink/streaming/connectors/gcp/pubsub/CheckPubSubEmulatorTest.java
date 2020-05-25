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
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

/**
 * Tests to ensure the docker image with PubSub is working correctly.
 */
public class CheckPubSubEmulatorTest extends GCloudUnitTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(CheckPubSubEmulatorTest.class);

	private static final String PROJECT_NAME = "Project";
	private static final String TOPIC_NAME = "Topic";
	private static final String SUBSCRIPTION_NAME = "Subscription";

	private static PubsubHelper pubsubHelper = getPubsubHelper();

	@BeforeClass
	public static void setUp() throws Exception {
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

		//TODO this is just to test if we need to wait longer, or if something has gone wrong and the message will never arrive
		if (receivedMessages.isEmpty()) {
			LOG.error("Message did not arrive, gonna wait 60s and try to pull again.");
			Thread.sleep(30 * 1000);
			receivedMessages = pubsubHelper.pullMessages(PROJECT_NAME, SUBSCRIPTION_NAME, 1);
		}
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
				(message, consumer) -> receivedMessages.add(message)
			);

		Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
		publisher
			.publish(PubsubMessage
				.newBuilder()
				.setData(ByteString.copyFromUtf8("Hello World"))
				.build())
			.get();

		LOG.info("Waiting a while to receive the message...");

		waitUntill(() -> receivedMessages.size() > 0);

		assertEquals(1, receivedMessages.size());
		assertEquals("Hello World", receivedMessages.get(0).getData().toStringUtf8());

		try {
			subscriber.stopAsync().awaitTerminated(100, MILLISECONDS);
		} catch (TimeoutException tme) {
			// Yeah, whatever. Don't care about clean shutdown here.
		}
		publisher.shutdown();
	}

	/*
	 * Returns when predicate returns true or if 10 seconds have passed
	 */
	private void waitUntill(Supplier<Boolean> predicate) {
		int retries = 0;

		while (!predicate.get() && retries < 100) {
			retries++;
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) { }
		}
	}

}
