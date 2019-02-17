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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannel;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
	private final long maxOutstandingElementCount;
	private final long maxOutstandingRequestBytes;
	private final String hostAndPort;

	DefaultPubSubSubscriberFactory(String hostAndPort, long maxOutstandingElementCount, long maxOutstandingRequestBytes) {
		this.hostAndPort = hostAndPort;
		this.maxOutstandingElementCount = maxOutstandingElementCount;
		this.maxOutstandingRequestBytes = maxOutstandingRequestBytes;
	}

	@Override
	public Subscriber getSubscriber(Credentials credentials, ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver) {
		FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
			.setMaxOutstandingElementCount(maxOutstandingElementCount)
			.setMaxOutstandingRequestBytes(maxOutstandingRequestBytes)
			.setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
			.build();

		Subscriber.Builder builder = Subscriber
				.newBuilder(ProjectSubscriptionName.of(projectSubscriptionName.getProject(), projectSubscriptionName.getSubscription()), messageReceiver)
				.setFlowControlSettings(flowControlSettings)
				.setCredentialsProvider(FixedCredentialsProvider.create(credentials));

		if (hostAndPort != null) {
			ManagedChannel managedChannel = ManagedChannelBuilder
				.forTarget(hostAndPort)
				.usePlaintext() // This is 'Ok' because this is ONLY used for testing.
				.build();
			TransportChannel transportChannel = GrpcTransportChannel.newBuilder().setManagedChannel(managedChannel).build();
			builder.setChannelProvider(FixedTransportChannelProvider.create(transportChannel))
					.setCredentialsProvider(NoCredentialsProvider.create());
		}

		return builder.build();
	}
}
