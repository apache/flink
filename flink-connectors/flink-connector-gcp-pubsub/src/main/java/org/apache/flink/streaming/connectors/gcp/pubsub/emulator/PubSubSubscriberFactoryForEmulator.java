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

import org.apache.flink.streaming.connectors.gcp.pubsub.BlockingGrpcPubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;

import com.google.auth.Credentials;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.time.Duration;

/**
 * A convenience PubSubSubscriberFactory that can be used to connect to a PubSub emulator. The
 * PubSub emulators do not support SSL or Credentials and as such this SubscriberStub does not
 * require or provide this.
 */
public class PubSubSubscriberFactoryForEmulator implements PubSubSubscriberFactory {
    private final String hostAndPort;
    private final String projectSubscriptionName;
    private final int retries;
    private final Duration timeout;
    private final int maxMessagesPerPull;

    public PubSubSubscriberFactoryForEmulator(
            String hostAndPort,
            String project,
            String subscription,
            int retries,
            Duration timeout,
            int maxMessagesPerPull) {
        this.hostAndPort = hostAndPort;
        this.retries = retries;
        this.timeout = timeout;
        this.maxMessagesPerPull = maxMessagesPerPull;
        this.projectSubscriptionName = ProjectSubscriptionName.format(project, subscription);
    }

    @Override
    public PubSubSubscriber getSubscriber(Credentials credentials) throws IOException {
        ManagedChannel managedChannel =
                NettyChannelBuilder.forTarget(hostAndPort)
                        .usePlaintext() // This is 'Ok' because this is ONLY used for testing.
                        .build();

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(maxMessagesPerPull)
                        .setSubscription(projectSubscriptionName)
                        .build();
        SubscriberGrpc.SubscriberBlockingStub stub = SubscriberGrpc.newBlockingStub(managedChannel);
        return new BlockingGrpcPubSubSubscriber(
                projectSubscriptionName, managedChannel, stub, pullRequest, retries, timeout);
    }
}
