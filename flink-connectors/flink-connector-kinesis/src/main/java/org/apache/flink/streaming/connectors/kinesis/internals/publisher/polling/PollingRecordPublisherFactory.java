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

package org.apache.flink.streaming.connectors.kinesis.internals.publisher.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher.FlinkKinesisProxyFactory;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisherFactory;
import org.apache.flink.streaming.connectors.kinesis.metrics.PollingRecordPublisherMetricsReporter;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

/** A {@link RecordPublisher} factory used to create instances of {@link PollingRecordPublisher}. */
@Internal
public class PollingRecordPublisherFactory implements RecordPublisherFactory {

    private final FlinkKinesisProxyFactory kinesisProxyFactory;

    public PollingRecordPublisherFactory(final FlinkKinesisProxyFactory kinesisProxyFactory) {
        this.kinesisProxyFactory = kinesisProxyFactory;
    }

    /**
     * Create a {@link PollingRecordPublisher}. An {@link AdaptivePollingRecordPublisher} will be
     * created should adaptive reads be enabled in the configuration.
     *
     * @param startingPosition the position in the shard to start consuming records from
     * @param consumerConfig the consumer configuration properties
     * @param metricGroup the metric group to report metrics to
     * @param streamShardHandle the shard this consumer is subscribed to
     * @return a {@link PollingRecordPublisher}
     */
    @Override
    public PollingRecordPublisher create(
            final StartingPosition startingPosition,
            final Properties consumerConfig,
            final MetricGroup metricGroup,
            final StreamShardHandle streamShardHandle)
            throws InterruptedException {
        Preconditions.checkNotNull(startingPosition);
        Preconditions.checkNotNull(consumerConfig);
        Preconditions.checkNotNull(metricGroup);
        Preconditions.checkNotNull(streamShardHandle);

        final PollingRecordPublisherConfiguration configuration =
                new PollingRecordPublisherConfiguration(consumerConfig);
        final PollingRecordPublisherMetricsReporter metricsReporter =
                new PollingRecordPublisherMetricsReporter(metricGroup);
        final KinesisProxyInterface kinesisProxy = kinesisProxyFactory.create(consumerConfig);

        if (configuration.isAdaptiveReads()) {
            return new AdaptivePollingRecordPublisher(
                    startingPosition,
                    streamShardHandle,
                    metricsReporter,
                    kinesisProxy,
                    configuration.getMaxNumberOfRecordsPerFetch(),
                    configuration.getFetchIntervalMillis());
        } else {
            return new PollingRecordPublisher(
                    startingPosition,
                    streamShardHandle,
                    metricsReporter,
                    kinesisProxy,
                    configuration.getMaxNumberOfRecordsPerFetch(),
                    configuration.getFetchIntervalMillis());
        }
    }
}
