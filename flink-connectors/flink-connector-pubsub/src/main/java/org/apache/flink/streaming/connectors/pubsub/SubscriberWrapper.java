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
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper class around a PubSub {@link Subscriber}.
 * This class makes it easier to connect to a Non Google PubSub service such as a local PubSub emulator or docker container.
 */
class SubscriberWrapper implements Serializable, MessageReceiver {
	private static final Logger LOG = LoggerFactory.getLogger(PubSubSource.class);

	private final SerializableCredentialsProvider serializableCredentialsProvider;
	private final String projectId;
	private final String subscriptionId;
	private final PubSubSubscriberFactory pubSubSubscriberFactory;

	private transient Subscriber subscriber;
	private transient BlockingQueue<Tuple2<PubsubMessage, AckReplyConsumer>> messageQueue;

	SubscriberWrapper(SerializableCredentialsProvider serializableCredentialsProvider, ProjectSubscriptionName projectSubscriptionName, PubSubSubscriberFactory pubSubSubscriberFactory) {
		this.serializableCredentialsProvider = serializableCredentialsProvider;
		this.projectId = projectSubscriptionName.getProject();
		this.subscriptionId = projectSubscriptionName.getSubscription();
		this.pubSubSubscriberFactory = pubSubSubscriberFactory;
	}

	void initialize() {
		this.subscriber = pubSubSubscriberFactory.getSubscriber(serializableCredentialsProvider, ProjectSubscriptionName.of(projectId, subscriptionId), this);
		this.messageQueue = new LinkedBlockingQueue<>();
	}

	void start() {
		ApiService apiService = subscriber.startAsync();
		apiService.awaitRunning();
	}

	void stop() {
		if (subscriber != null) {
			subscriber.stopAsync().awaitTerminated();
		}
	}

	Tuple2<PubsubMessage, AckReplyConsumer> take() throws InterruptedException {
		return messageQueue.poll(1000L, TimeUnit.MILLISECONDS);
	}

	private void nackAllMessagesInBuffer() throws InterruptedException {
		LOG.debug("Going to nack {} messages.", amountOfMessagesInBuffer());
		while (!messageQueue.isEmpty()) {
			Optional.ofNullable(messageQueue.poll(1000L, TimeUnit.MILLISECONDS))
					.map(tuple -> tuple.f1)
					.ifPresent(AckReplyConsumer::nack);
		}
		LOG.debug("Finished nacking messages in buffer.");
	}

	int amountOfMessagesInBuffer() {
		return messageQueue.size();
	}

	boolean isRunning() {
		return subscriber.isRunning();
	}

	void awaitTerminated() throws Exception {
		nackAllMessagesInBuffer();
		subscriber.awaitTerminated();
	}

	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		if (subscriber.isRunning()) {
			messageQueue.offer(Tuple2.of(message, consumer));
		} else {
			consumer.nack();
		}
	}

	Subscriber getSubscriber() {
		return subscriber;
	}
}
