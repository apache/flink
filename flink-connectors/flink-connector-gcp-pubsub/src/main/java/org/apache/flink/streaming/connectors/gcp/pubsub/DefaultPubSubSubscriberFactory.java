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
import org.apache.flink.streaming.connectors.gcp.pubsub.source.PubSubSource;

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

/**
 * A default {@link PubSubSubscriberFactory} used by the {@link PubSubSource.PubSubSourceBuilder} to
 * obtain a subscriber with which messages can be pulled from GCP Pub/Sub.
 */
public class DefaultPubSubSubscriberFactory implements PubSubSubscriberFactory {
    private final int retries;
    private final Duration timeout;
    private final int maxMessagesPerPull;
    private final String projectSubscriptionName;

    /**
     * @param projectSubscriptionName The formatted name of the Pub/Sub project and subscription to
     *     pull messages from. Can be easily obtained through {@link
     *     com.google.pubsub.v1.ProjectSubscriptionName}.
     * @param retries The number of times the reception of a message should be retried in case of
     *     failure.
     * @param pullTimeout The timeout after which a message pull request is deemed a failure
     * @param maxMessagesPerPull The maximum number of messages that should be pulled in one go.
     */
    public DefaultPubSubSubscriberFactory(
            String projectSubscriptionName,
            int retries,
            Duration pullTimeout,
            int maxMessagesPerPull) {
        this.retries = retries;
        this.timeout = pullTimeout;
        this.maxMessagesPerPull = maxMessagesPerPull;
        this.projectSubscriptionName = projectSubscriptionName;
    }

    @Override
    public PubSubSubscriber getSubscriber(Credentials credentials) throws IOException {
        ManagedChannel channel =
                NettyChannelBuilder.forTarget(SubscriberStubSettings.getDefaultEndpoint())
                        .negotiationType(NegotiationType.TLS)
                        .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
                        .build();

        PullRequest pullRequest =
                PullRequest.newBuilder()
                        .setMaxMessages(maxMessagesPerPull)
                        .setSubscription(projectSubscriptionName)
                        .build();
        SubscriberGrpc.SubscriberBlockingStub stub =
                SubscriberGrpc.newBlockingStub(channel)
                        .withCallCredentials(MoreCallCredentials.from(credentials));
        return new BlockingGrpcPubSubSubscriber(
                projectSubscriptionName, channel, stub, pullRequest, retries, timeout);
    }
}
