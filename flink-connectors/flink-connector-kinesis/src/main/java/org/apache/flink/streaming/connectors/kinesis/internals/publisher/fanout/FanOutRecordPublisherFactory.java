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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.FullJitterBackoff;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.util.Optional;
import java.util.Properties;

import static java.util.Collections.singletonList;

/** A {@link RecordPublisher} factory used to create instances of {@link FanOutRecordPublisher}. */
@Internal
public class FanOutRecordPublisherFactory implements RecordPublisherFactory {

    private static final FullJitterBackoff BACKOFF = new FullJitterBackoff();

    /**
     * A singleton {@link KinesisProxyV2} is used per Flink task. The {@link KinesisAsyncClient}
     * uses an internal thread pool; using a single client reduces overhead.
     */
    private final KinesisProxyV2Interface kinesisProxy;

    /**
     * Instantiate a factory responsible for creating {@link FanOutRecordPublisher}.
     *
     * @param kinesisProxy the singleton proxy used by all record publishers created by this factory
     */
    public FanOutRecordPublisherFactory(final KinesisProxyV2Interface kinesisProxy) {
        this.kinesisProxy = kinesisProxy;
    }

    /**
     * Create a {@link FanOutRecordPublisher}.
     *
     * @param startingPosition the starting position in the shard to start consuming from
     * @param consumerConfig the consumer configuration properties
     * @param metricGroup the metric group to report metrics to
     * @param streamShardHandle the shard this consumer is subscribed to
     * @return a {@link FanOutRecordPublisher}
     */
    @Override
    public FanOutRecordPublisher create(
            final StartingPosition startingPosition,
            final Properties consumerConfig,
            final MetricGroup metricGroup,
            final StreamShardHandle streamShardHandle) {
        Preconditions.checkNotNull(startingPosition);
        Preconditions.checkNotNull(consumerConfig);
        Preconditions.checkNotNull(metricGroup);
        Preconditions.checkNotNull(streamShardHandle);

        String stream = streamShardHandle.getStreamName();
        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(consumerConfig, singletonList(stream));

        Optional<String> streamConsumerArn = configuration.getStreamConsumerArn(stream);
        Preconditions.checkState(streamConsumerArn.isPresent());

        return new FanOutRecordPublisher(
                startingPosition,
                streamConsumerArn.get(),
                streamShardHandle,
                kinesisProxy,
                configuration,
                BACKOFF);
    }

    @Override
    public void close() {
        kinesisProxy.close();
    }
}
