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

import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS;
import static org.apache.flink.streaming.connectors.kinesis.internals.ShardConsumerTestUtils.createFakeShardConsumerMetricGroup;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for {@link PollingRecordPublisherFactory}. */
public class PollingRecordPublisherFactoryTest {

    private final PollingRecordPublisherFactory factory =
            new PollingRecordPublisherFactory(props -> mock(KinesisProxy.class));

    @Test
    public void testBuildPollingRecordPublisher() throws Exception {
        RecordPublisher recordPublisher =
                factory.create(
                        StartingPosition.restartFromSequenceNumber(
                                SENTINEL_LATEST_SEQUENCE_NUM.get()),
                        new Properties(),
                        createFakeShardConsumerMetricGroup(),
                        mock(StreamShardHandle.class));

        assertThat(recordPublisher).isInstanceOf(PollingRecordPublisher.class);
        assertThat(recordPublisher).isNotInstanceOf(AdaptivePollingRecordPublisher.class);
    }

    @Test
    public void testBuildAdaptivePollingRecordPublisher() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(SHARD_USE_ADAPTIVE_READS, "true");

        RecordPublisher recordPublisher =
                factory.create(
                        StartingPosition.restartFromSequenceNumber(
                                SENTINEL_LATEST_SEQUENCE_NUM.get()),
                        properties,
                        createFakeShardConsumerMetricGroup(),
                        mock(StreamShardHandle.class));

        assertThat(recordPublisher).isInstanceOf(PollingRecordPublisher.class);
        assertThat(recordPublisher).isInstanceOf(AdaptivePollingRecordPublisher.class);
    }
}
