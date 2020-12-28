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

import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisFanOutBehavioursFactory.SubscriptionErrorKinesisV2;

import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;

/** Tests for {@link FanOutShardSubscriber}. */
public class FanOutShardSubscriberTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRecoverableErrorThrownToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.RecoverableFanOutSubscriberException.class);
        thrown.expectMessage("io.netty.handler.timeout.ReadTimeoutException");

        SubscriptionErrorKinesisV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(
                        ReadTimeoutException.INSTANCE);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber("consumerArn", "shardId", errorKinesisV2);

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testRetryableErrorThrownToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.RetryableFanOutSubscriberException.class);
        thrown.expectMessage("Error!");

        RuntimeException error = new RuntimeException("Error!");
        SubscriptionErrorKinesisV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(error);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber("consumerArn", "shardId", errorKinesisV2);

        software.amazon.awssdk.services.kinesis.model.StartingPosition startingPosition =
                software.amazon.awssdk.services.kinesis.model.StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }

    @Test
    public void testMultipleErrorsThrownPassesFirstErrorToConsumer() throws Exception {
        thrown.expect(FanOutShardSubscriber.FanOutSubscriberException.class);
        thrown.expectMessage("Error 1!");

        RuntimeException error1 = new RuntimeException("Error 1!");
        RuntimeException error2 = new RuntimeException("Error 2!");
        SubscriptionErrorKinesisV2 errorKinesisV2 =
                FakeKinesisFanOutBehavioursFactory.errorDuringSubscription(error1, error2);

        FanOutShardSubscriber subscriber =
                new FanOutShardSubscriber("consumerArn", "shardId", errorKinesisV2);

        StartingPosition startingPosition = StartingPosition.builder().build();
        subscriber.subscribeToShardAndConsumeRecords(startingPosition, event -> {});
    }
}
