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

import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;

import java.io.IOException;

/**
 * A convenience PubSubSubscriberFactory that can be used to connect to a PubSub emulator.
 * The PubSub emulators do not support SSL or Credentials and as such this SubscriberStub does not require or provide this.
 */
public class PubSubSubscriberFactoryForEmulator implements PubSubSubscriberFactory {
	private final String hostAndPort;

	public PubSubSubscriberFactoryForEmulator(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}

	@Override
	public SubscriberStub getSubscriber(EventLoopGroup eventLoopGroup, Credentials credentials) throws IOException {
		ManagedChannel managedChannel = NettyChannelBuilder.forTarget(hostAndPort)
														.usePlaintext() // This is 'Ok' because this is ONLY used for testing.
														.eventLoopGroup(eventLoopGroup)
														.build();

		SubscriberStubSettings settings = SubscriberStubSettings.newBuilder()
																.setCredentialsProvider(NoCredentialsProvider.create())
																.setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel)))
																.build();
		return GrpcSubscriberStub.create(settings);
	}
}
