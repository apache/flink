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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.metrics.KinesisConsumerMetricConstants;
import org.apache.flink.streaming.connectors.kinesis.metrics.ShardConsumerMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestSourceContext;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.lang3.StringUtils;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_SHARD_ENDING_SEQUENCE_NUM;
import static org.junit.Assert.assertEquals;

/** Tests for the {@link ShardConsumer}. */
public class ShardConsumerTestUtils {

    public static ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
            final int expectedNumberOfMessages,
            final RecordPublisherFactory recordPublisherFactory,
            final SequenceNumber startingSequenceNumber,
            final Properties consumerProperties)
            throws InterruptedException {
        return assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfMessages,
                recordPublisherFactory,
                startingSequenceNumber,
                consumerProperties,
                SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get());
    }

    public static ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
            final int expectedNumberOfMessages,
            final RecordPublisherFactory recordPublisherFactory,
            final SequenceNumber startingSequenceNumber,
            final Properties consumerProperties,
            final AbstractMetricGroup metricGroup)
            throws InterruptedException {
        return assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfMessages,
                recordPublisherFactory,
                startingSequenceNumber,
                consumerProperties,
                SENTINEL_SHARD_ENDING_SEQUENCE_NUM.get(),
                metricGroup);
    }

    public static ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
            final int expectedNumberOfMessages,
            final RecordPublisherFactory recordPublisherFactory,
            final SequenceNumber startingSequenceNumber,
            final Properties consumerProperties,
            final SequenceNumber expectedLastProcessedSequenceNum,
            final AbstractMetricGroup metricGroup)
            throws InterruptedException {
        ShardConsumerMetricsReporter shardMetricsReporter =
                new ShardConsumerMetricsReporter(metricGroup);

        StreamShardHandle fakeToBeConsumedShard = getMockStreamShard("fakeStream", 0);

        LinkedList<KinesisStreamShardState> subscribedShardsStateUnderTest = new LinkedList<>();
        subscribedShardsStateUnderTest.add(
                new KinesisStreamShardState(
                        KinesisDataFetcher.convertToStreamShardMetadata(fakeToBeConsumedShard),
                        fakeToBeConsumedShard,
                        startingSequenceNumber));

        TestSourceContext<String> sourceContext = new TestSourceContext<>();

        KinesisDeserializationSchemaWrapper<String> deserializationSchema =
                new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema());
        TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        Collections.singletonList("fakeStream"),
                        sourceContext,
                        consumerProperties,
                        deserializationSchema,
                        10,
                        2,
                        new AtomicReference<>(),
                        subscribedShardsStateUnderTest,
                        KinesisDataFetcher
                                .createInitialSubscribedStreamsToLastDiscoveredShardsState(
                                        Collections.singletonList("fakeStream")),
                        Mockito.mock(KinesisProxyInterface.class),
                        Mockito.mock(KinesisProxyV2Interface.class));

        final StreamShardHandle shardHandle =
                subscribedShardsStateUnderTest.get(0).getStreamShardHandle();
        final SequenceNumber lastProcessedSequenceNum =
                subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum();
        final StartingPosition startingPosition =
                AWSUtil.getStartingPosition(lastProcessedSequenceNum, consumerProperties);

        final RecordPublisher recordPublisher =
                recordPublisherFactory.create(
                        startingPosition,
                        fetcher.getConsumerConfiguration(),
                        metricGroup,
                        shardHandle);

        int shardIndex =
                fetcher.registerNewSubscribedShardState(subscribedShardsStateUnderTest.get(0));
        new ShardConsumer<>(
                        fetcher,
                        recordPublisher,
                        shardIndex,
                        shardHandle,
                        lastProcessedSequenceNum,
                        shardMetricsReporter,
                        deserializationSchema)
                .run();

        assertEquals(expectedNumberOfMessages, sourceContext.getCollectedOutputs().size());
        assertEquals(
                expectedLastProcessedSequenceNum,
                subscribedShardsStateUnderTest.get(0).getLastProcessedSequenceNum());

        return shardMetricsReporter;
    }

    public static ShardConsumerMetricsReporter assertNumberOfMessagesReceivedFromKinesis(
            final int expectedNumberOfMessages,
            final RecordPublisherFactory recordPublisherFactory,
            final SequenceNumber startingSequenceNumber,
            final Properties consumerProperties,
            final SequenceNumber expectedLastProcessedSequenceNum)
            throws InterruptedException {
        return assertNumberOfMessagesReceivedFromKinesis(
                expectedNumberOfMessages,
                recordPublisherFactory,
                startingSequenceNumber,
                consumerProperties,
                expectedLastProcessedSequenceNum,
                createFakeShardConsumerMetricGroup());
    }

    public static StreamShardHandle getMockStreamShard(String streamName, int shardId) {
        return new StreamShardHandle(
                streamName,
                new Shard()
                        .withShardId(KinesisShardIdGenerator.generateFromShardOrder(shardId))
                        .withHashKeyRange(
                                new HashKeyRange()
                                        .withStartingHashKey("0")
                                        .withEndingHashKey(
                                                new BigInteger(StringUtils.repeat("FF", 16), 16)
                                                        .toString())));
    }

    public static SequenceNumber fakeSequenceNumber() {
        return new SequenceNumber("fakeStartingState");
    }

    public static AbstractMetricGroup createFakeShardConsumerMetricGroup(
            OperatorMetricGroup metricGroup) {
        return (AbstractMetricGroup)
                metricGroup
                        .addGroup(KinesisConsumerMetricConstants.STREAM_METRICS_GROUP, "fakeStream")
                        .addGroup(
                                KinesisConsumerMetricConstants.SHARD_METRICS_GROUP,
                                "shardId-000000000000");
    }

    public static AbstractMetricGroup createFakeShardConsumerMetricGroup() {
        return createFakeShardConsumerMetricGroup(
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup());
    }
}
