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

import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.ManagedChannel;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.time.Duration;

class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
	private final int retries;
	private final Duration timeout;
	private final int maxMessagesPerPull;
	private final String projectSubscriptionName;

	DefaultPubSubSubscriberFactory(String projectSubscriptionName, int retries, Duration pullTimeout, int maxMessagesPerPull) {
		this.retries = retries;
		this.timeout = pullTimeout;
		this.maxMessagesPerPull = maxMessagesPerPull;
		this.projectSubscriptionName = projectSubscriptionName;
	}

	@Override
	public PubSubSubscriber getSubscriber(Credentials credentials) throws IOException {
		ManagedChannel channel = NettyChannelBuilder.forTarget(SubscriberStubSettings.getDefaultEndpoint())
													.negotiationType(NegotiationType.TLS)
													.sslContext(GrpcSslContexts.forClient().ciphers(null).build())
													.build();

		PullRequest pullRequest = PullRequest.newBuilder()
								.setMaxMessages(maxMessagesPerPull)
								.setSubscription(projectSubscriptionName)
								.build();
		SubscriberGrpc.SubscriberBlockingStub stub = SubscriberGrpc.newBlockingStub(channel)
							.withCallCredentials(MoreCallCredentials.from(credentials));
		return new BlockingGrpcPubSubSubscriber(projectSubscriptionName, channel, stub, pullRequest, retries, timeout);
	}

}
