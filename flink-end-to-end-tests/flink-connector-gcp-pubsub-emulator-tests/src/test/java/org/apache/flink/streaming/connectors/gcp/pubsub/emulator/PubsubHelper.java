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

package org.apache.flink.streaming.connectors.gcp.pubsub.emulator;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


/**
 * A helper class to make managing the testing topics a bit easier.
 */
public class PubsubHelper {

	private static final Logger LOG = LoggerFactory.getLogger(PubsubHelper.class);

	private TransportChannelProvider channelProvider;

	private TopicAdminClient topicClient;
	private SubscriptionAdminClient subscriptionAdminClient;

	public PubsubHelper(TransportChannelProvider channelProvider) {
		this.channelProvider = channelProvider;
	}

	public TopicAdminClient getTopicAdminClient() throws IOException {
		if (topicClient == null) {
			TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
				.setTransportChannelProvider(channelProvider)
				.setCredentialsProvider(NoCredentialsProvider.create())
				.build();
			topicClient = TopicAdminClient.create(topicAdminSettings);
		}
		return topicClient;
	}

	public Topic createTopic(String project, String topic) throws IOException {
		deleteTopic(project, topic);
		ProjectTopicName topicName = ProjectTopicName.of(project, topic);
		TopicAdminClient adminClient = getTopicAdminClient();
		LOG.info("CreateTopic {}", topicName);
		return adminClient.createTopic(topicName);
	}

	public void deleteTopic(String project, String topic) throws IOException {
		deleteTopic(ProjectTopicName.of(project, topic));
	}

	public void deleteTopic(ProjectTopicName topicName) throws IOException {
		TopicAdminClient adminClient = getTopicAdminClient();
		try {
			adminClient.getTopic(topicName);
		} catch (NotFoundException e) {
			// Doesn't exist. Good.
			return;
		}

		// If it exists we delete all subscriptions and the topic itself.
		LOG.info("DeleteTopic {} first delete old subscriptions.", topicName);
		adminClient
			.listTopicSubscriptions(topicName)
			.iterateAllAsProjectSubscriptionName()
			.forEach(subscriptionAdminClient::deleteSubscription);
		LOG.info("DeleteTopic {}", topicName);
		adminClient
			.deleteTopic(topicName);
	}

	public SubscriptionAdminClient getSubscriptionAdminClient() throws IOException {
		if (subscriptionAdminClient == null) {
			SubscriptionAdminSettings subscriptionAdminSettings =
				SubscriptionAdminSettings
					.newBuilder()
					.setTransportChannelProvider(channelProvider)
					.setCredentialsProvider(NoCredentialsProvider.create())
					.build();
			subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
		}
		return subscriptionAdminClient;
	}

	public void createSubscription(String subscriptionProject, String subscription, String topicProject, String topic) throws IOException {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.newBuilder()
			.setProject(subscriptionProject)
			.setSubscription(subscription)
			.build();

		deleteSubscription(subscriptionName);

		ProjectTopicName topicName = ProjectTopicName.of(topicProject, topic);

		PushConfig pushConfig = PushConfig.getDefaultInstance();

		LOG.info("CreateSubscription {}", subscriptionName);
		getSubscriptionAdminClient().createSubscription(subscriptionName, topicName, pushConfig, 1).isInitialized();
	}

	public void deleteSubscription(String subscriptionProject, String subscription) throws IOException {
		deleteSubscription(ProjectSubscriptionName
			.newBuilder()
			.setProject(subscriptionProject)
			.setSubscription(subscription)
			.build());
	}

	public void deleteSubscription(ProjectSubscriptionName subscriptionName) throws IOException {
		SubscriptionAdminClient adminClient = getSubscriptionAdminClient();
		try {
			adminClient.getSubscription(subscriptionName);
			// If it already exists we must first delete it.
			LOG.info("DeleteSubscription {}", subscriptionName);
			adminClient.deleteSubscription(subscriptionName);
		} catch (NotFoundException e) {
			// Doesn't exist. Good.
		}
	}

	//
	// Mostly copied from the example on https://cloud.google.com/pubsub/docs/pull
	// Licensed under the Apache 2.0 License to "Google LLC" from https://github.com/googleapis/google-cloud-java/blob/master/google-cloud-examples/src/main/java/com/google/cloud/examples/pubsub/snippets/SubscriberSnippets.java.
	//
	public List<ReceivedMessage> pullMessages(String projectId, String subscriptionId, int maxNumberOfMessages) throws Exception {
		SubscriberStubSettings subscriberStubSettings =
			SubscriberStubSettings.newBuilder()
				.setTransportChannelProvider(channelProvider)
				.setCredentialsProvider(NoCredentialsProvider.create())
				.build();
		try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
			String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
			PullRequest pullRequest =
				PullRequest.newBuilder()
					.setMaxMessages(maxNumberOfMessages)
					.setReturnImmediately(false)
					.setSubscription(subscriptionName)
					.build();

			List<ReceivedMessage> receivedMessages = subscriber.pullCallable().call(pullRequest).getReceivedMessagesList();
			acknowledgeIds(subscriber, subscriptionName, receivedMessages);
			return receivedMessages;
		}
	}

	private void acknowledgeIds(SubscriberStub subscriber, String subscriptionName, List<ReceivedMessage> receivedMessages) {
		if (receivedMessages.isEmpty()) {
			return;
		}

		List<String> ackIds = receivedMessages.stream().map(ReceivedMessage::getAckId).collect(Collectors.toList());
		// acknowledge received messages
		AcknowledgeRequest acknowledgeRequest =
			AcknowledgeRequest.newBuilder()
							.setSubscription(subscriptionName)
							.addAllAckIds(ackIds)
							.build();
		// use acknowledgeCallable().futureCall to asynchronously perform this operation
		subscriber.acknowledgeCallable().call(acknowledgeRequest);
	}

	public Subscriber subscribeToSubscription(String project, String subscription, MessageReceiver messageReceiver) {
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(project, subscription);
		Subscriber subscriber =
			Subscriber
				.newBuilder(subscriptionName, messageReceiver)
				.setChannelProvider(channelProvider)
				.setCredentialsProvider(NoCredentialsProvider.create())
				.build();
		subscriber.startAsync();
		return subscriber;
	}

	public Publisher createPublisher(String project, String topic) throws IOException {
		return Publisher
			.newBuilder(ProjectTopicName.of(project, topic))
			.setChannelProvider(channelProvider)
			.setCredentialsProvider(NoCredentialsProvider.create())
			.build();
	}

}
