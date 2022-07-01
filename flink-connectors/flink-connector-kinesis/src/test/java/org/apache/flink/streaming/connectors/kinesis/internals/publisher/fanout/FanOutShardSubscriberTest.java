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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout;

import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.SubscriptionErrorKinesisV2;

import com.amazonaws.http.timers.client.SdkInterruptedException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;

import java.time.Duration;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FanOutShardSubscriber}. */
public class FanOutShardSubscriberTest {

    @Test
    public void testRecoverableErrorThrownToConsumer() {
        assertThatThrownBy(
                        () -> {
                            SubscriptionErrorKinesisV2 errorKinesisV2 =
                                    FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                                            ReadTimeoutException.INSTANCE);

                            FanOutShardSubscriber subscriber =
                                    new FanOutShardSubscriber(
                                            "consumerArn",
                                            "shardId",
                                            errorKinesisV2,
                                            DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT);

                            software.amazon.awssdk.services.kinesis.model.StartingPosition
                                    startingPosition =
                                            software.amazon.awssdk.services.kinesis.model
                                                    .StartingPosition.builder()
                                                    .build();
                            subscriber.subscribeToShardAndConsumeRecords(
                                    startingPosition, event -> {});
                        })
                .isInstanceOf(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class)
                .hasMessageContaining("io.netty.handler.timeout.ReadTimeoutException");
    }

    @Test
    public void testRetryableErrorThrownToConsumer() {
        assertThatThrownBy(
                        () -> {
                            RuntimeException error = new RuntimeException("Error!");
                            SubscriptionErrorKinesisV2 errorKinesisV2 =
                                    FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                                            error);

                            FanOutShardSubscriber subscriber =
                                    new FanOutShardSubscriber(
                                            "consumerArn",
                                            "shardId",
                                            errorKinesisV2,
                                            DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT);

                            software.amazon.awssdk.services.kinesis.model.StartingPosition
                                    startingPosition =
                                            software.amazon.awssdk.services.kinesis.model
                                                    .StartingPosition.builder()
                                                    .build();
                            subscriber.subscribeToShardAndConsumeRecords(
                                    startingPosition, event -> {});
                        })
                .isInstanceOf(FanOutShardSubscriber.RetryableFanOutSubscriberException.class)
                .hasMessageContaining("Error!");
    }

    @Test
    public void testInterruptedErrorThrownToConsumer() {
        assertThatThrownBy(
                        () -> {
                            SdkInterruptedException error = new SdkInterruptedException(null);
                            SubscriptionErrorKinesisV2 errorKinesisV2 =
                                    FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                                            error);

                            FanOutShardSubscriber subscriber =
                                    new FanOutShardSubscriber(
                                            "consumerArn",
                                            "shardId",
                                            errorKinesisV2,
                                            DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT);

                            software.amazon.awssdk.services.kinesis.model.StartingPosition
                                    startingPosition =
                                            software.amazon.awssdk.services.kinesis.model
                                                    .StartingPosition.builder()
                                                    .build();
                            subscriber.subscribeToShardAndConsumeRecords(
                                    startingPosition, event -> {});
                        })
                .isInstanceOf(FanOutShardSubscriber.FanOutSubscriberInterruptedException.class);
    }

    @Test
    public void testMultipleErrorsThrownPassesFirstErrorToConsumer() {
        assertThatThrownBy(
                        () -> {
                            RuntimeException error1 = new RuntimeException("Error 1!");
                            RuntimeException error2 = new RuntimeException("Error 2!");
                            SubscriptionErrorKinesisV2 errorKinesisV2 =
                                    FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                                            error1, error2);

                            FanOutShardSubscriber subscriber =
                                    new FanOutShardSubscriber(
                                            "consumerArn",
                                            "shardId",
                                            errorKinesisV2,
                                            DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT);

                            StartingPosition startingPosition = StartingPosition.builder().build();
                            subscriber.subscribeToShardAndConsumeRecords(
                                    startingPosition, event -> {});
                        })
                .isInstanceOf(FanOutShardSubscriber.FanOutSubscriberException.class)
                .hasMessageContaining("Error 1!");
    }

    @Test
    public void testTimeoutSubscribingToShard() {
        assertThatThrownBy(
                        () -> {
                            KinesisProxyV2Interface kinesis =
                                    FakeKinesisFanOutBehavioursFactory.failsToAcquireSubscription();

                            FanOutShardSubscriber subscriber =
                                    new FanOutShardSubscriber(
                                            "consumerArn",
                                            "shardId",
                                            kinesis,
                                            Duration.ofMillis(1));

                            StartingPosition startingPosition = StartingPosition.builder().build();
                            subscriber.subscribeToShardAndConsumeRecords(
                                    startingPosition, event -> {});
                        })
                .isInstanceOf(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class)
                .hasMessageContaining("Timed out acquiring subscription");
    }

    @Test
    public void testTimeoutEnqueuingEvent() {
        assertThatThrownBy(
                        () -> {
                            KinesisProxyV2Interface kinesis =
                                    FakeKinesisFanOutBehavioursFactory
                                            .shardThatCreatesBackpressureOnQueue();

                            FanOutShardSubscriber subscriber =
                                    new FanOutShardSubscriber(
                                            "consumerArn",
                                            "shardId",
                                            kinesis,
                                            DEFAULT_SUBSCRIBE_TO_SHARD_TIMEOUT,
                                            Duration.ofMillis(100));

                            StartingPosition startingPosition = StartingPosition.builder().build();
                            subscriber.subscribeToShardAndConsumeRecords(
                                    startingPosition,
                                    event -> {
                                        try {
                                            Thread.sleep(120);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    });
                        })
                .isInstanceOf(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class)
                .hasMessageContaining("Timed out enqueuing event SubscriptionNextEvent");
    }
}
