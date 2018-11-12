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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider;

import com.google.api.core.ApiService;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.apache.flink.api.java.ClosureCleaner.ensureSerializable;
import static org.apache.flink.streaming.connectors.pubsub.common.SerializableCredentialsProvider.withoutCredentials;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link SubscriberWrapper}.
 */
@RunWith(MockitoJUnitRunner.class)
public class SubscriberWrapperTest {
	@Mock
	private PubSubSubscriberFactory pubSubSubscriberFactory;

	@Mock
	private Subscriber subscriber;

	@Mock
	private ApiService apiService;

	private PubsubMessage pubsubMessage = pubSubMessage();

	@Mock
	private AckReplyConsumer ackReplyConsumer;

	private SubscriberWrapper subscriberWrapper;

	@Before
	public void setup() throws Exception {
		when(pubSubSubscriberFactory.getSubscriber(any(), any(), any())).thenReturn(subscriber);
		subscriberWrapper = new SubscriberWrapper(withoutCredentials(), ProjectSubscriptionName.of("projectId", "subscriptionId"), pubSubSubscriberFactory);
	}

	@Test
	public void testSerializedSubscriberBuilder() throws Exception {
		SubscriberWrapper subscriberWrapper = new SubscriberWrapper(withoutCredentials(), ProjectSubscriptionName.of("projectId", "subscriptionId"), SubscriberWrapperTest::subscriberFactory);
		ensureSerializable(subscriberWrapper);
	}

	@Test
	public void testInitialisation() {
		SerializableCredentialsProvider credentialsProvider = withoutCredentials();
		ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of("projectId", "subscriptionId");
		SubscriberWrapper subscriberWrapper = new SubscriberWrapper(credentialsProvider, projectSubscriptionName, pubSubSubscriberFactory);

		subscriberWrapper.initialize();
		verify(pubSubSubscriberFactory, times(1)).getSubscriber(credentialsProvider, projectSubscriptionName, subscriberWrapper);
	}

	@Test
	public void testStart() {
		when(subscriber.startAsync()).thenReturn(apiService);
		subscriberWrapper.initialize();

		subscriberWrapper.start();
		verify(apiService, times(1)).awaitRunning();
		assertThat(subscriberWrapper.amountOfMessagesInBuffer(), is(0));
	}

	@Test
	public void testStopWithoutInitialize() {
		subscriberWrapper.stop();
		verifyZeroInteractions(subscriber);
	}

	@Test
	public void testStopWithInitialize() {
		when(subscriber.stopAsync()).thenReturn(apiService);
		subscriberWrapper.initialize();

		subscriberWrapper.stop();
		verify(subscriber, times(1)).stopAsync();
		verify(apiService, times(1)).awaitTerminated();
	}

	@Test
	public void testReceivingMessage() throws Exception {
		when(subscriber.isRunning()).thenReturn(true);
		subscriberWrapper.initialize();

		subscriberWrapper.receiveMessage(pubsubMessage, ackReplyConsumer);
		assertThat(subscriberWrapper.take(), is(Tuple2.of(pubsubMessage, ackReplyConsumer)));
	}

	@Test
	public void testIsRunning() {
		subscriberWrapper.initialize();
		when(subscriber.isRunning()).thenReturn(true);

		boolean isRunning = subscriberWrapper.isRunning();
		assertThat(isRunning, is(true));
		verify(subscriber, times(1)).isRunning();
	}

	@Test
	public void testAwaitTerminated() throws Exception {
		subscriberWrapper.initialize();

		//create outstanding message
		subscriberWrapper.receiveMessage(pubsubMessage, ackReplyConsumer);

		subscriberWrapper.awaitTerminated();

		verify(ackReplyConsumer, times(1)).nack();
		verify(subscriber, times(1)).awaitTerminated();
	}

	@Test
	public void testAmountOfMessagesInBuffer() {
		when(subscriber.isRunning()).thenReturn(true);
		subscriberWrapper.initialize();

		subscriberWrapper.receiveMessage(pubsubMessage, ackReplyConsumer);
		subscriberWrapper.receiveMessage(pubsubMessage, ackReplyConsumer);
		subscriberWrapper.receiveMessage(pubsubMessage, ackReplyConsumer);

		assertThat(subscriberWrapper.amountOfMessagesInBuffer(), is(3));
	}

	private PubsubMessage pubSubMessage() {
		return PubsubMessage.newBuilder()
			.setData(ByteString.copyFrom("some message".getBytes()))
			.build();
	}

	private static Subscriber subscriberFactory(CredentialsProvider credentialsProvider, ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) {
		return null;
	}
}
