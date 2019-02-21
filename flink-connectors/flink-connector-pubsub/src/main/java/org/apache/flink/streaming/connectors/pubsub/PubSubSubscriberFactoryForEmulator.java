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

import org.apache.flink.streaming.connectors.pubsub.common.PubSubSubscriberFactory;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

/**
 * A PubSubSubscriberFactory that can be used to connect to a PubSub emulator.
 */
public class PubSubSubscriberFactoryForEmulator implements PubSubSubscriberFactory {
	private final String hostAndPort;

	public PubSubSubscriberFactoryForEmulator(String hostAndPort) {
		this.hostAndPort = hostAndPort;
	}

	@Override
	public SubscriberStub getSubscriber(Credentials credentials) throws IOException {
		ManagedChannel managedChannel = ManagedChannelBuilder
			.forTarget(hostAndPort)
			.usePlaintext() // This is 'Ok' because this is ONLY used for testing.
			.build();

		SubscriberStubSettings settings = SubscriberStubSettings.newBuilder()
																.setCredentialsProvider(NoCredentialsProvider.create())
																.setTransportChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel)))
																.build();
		return GrpcSubscriberStub.create(settings);
	}
}
