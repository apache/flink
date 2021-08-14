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

package org.apache.flink.connector.kafka.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.PARTITION_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.TOPIC_GROUP;
import static org.junit.Assert.assertEquals;

/** Unit test for {@link KafkaSourceReaderMetrics}. */
public class KafkaSourceReaderMetricsTest {

    private static final TopicPartition FOO_0 = new TopicPartition("foo", 0);
    private static final TopicPartition FOO_1 = new TopicPartition("foo", 1);
    private static final TopicPartition BAR_0 = new TopicPartition("bar", 0);
    private static final TopicPartition BAR_1 = new TopicPartition("bar", 1);

    @Test
    public void testCurrentOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(metricListener.getMetricGroup());

        kafkaSourceReaderMetrics.registerTopicPartition(FOO_0);
        kafkaSourceReaderMetrics.registerTopicPartition(FOO_1);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_0);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_1);

        kafkaSourceReaderMetrics.recordCurrentOffset(FOO_0, 15213L);
        kafkaSourceReaderMetrics.recordCurrentOffset(FOO_1, 18213L);
        kafkaSourceReaderMetrics.recordCurrentOffset(BAR_0, 18613L);
        kafkaSourceReaderMetrics.recordCurrentOffset(BAR_1, 15513L);

        assertCurrentOffset(FOO_0, 15213L, metricListener);
        assertCurrentOffset(FOO_1, 18213L, metricListener);
        assertCurrentOffset(BAR_0, 18613L, metricListener);
        assertCurrentOffset(BAR_1, 15513L, metricListener);
    }

    @Test
    public void testCommitOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(metricListener.getMetricGroup());

        kafkaSourceReaderMetrics.registerTopicPartition(FOO_0);
        kafkaSourceReaderMetrics.registerTopicPartition(FOO_1);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_0);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_1);

        kafkaSourceReaderMetrics.recordCommittedOffset(FOO_0, 15213L);
        kafkaSourceReaderMetrics.recordCommittedOffset(FOO_1, 18213L);
        kafkaSourceReaderMetrics.recordCommittedOffset(BAR_0, 18613L);
        kafkaSourceReaderMetrics.recordCommittedOffset(BAR_1, 15513L);

        assertCommittedOffset(FOO_0, 15213L, metricListener);
        assertCommittedOffset(FOO_1, 18213L, metricListener);
        assertCommittedOffset(BAR_0, 18613L, metricListener);
        assertCommittedOffset(BAR_1, 15513L, metricListener);

        assertEquals(
                0L,
                metricListener
                        .getCounter(
                                KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                                KafkaSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER)
                        .getCount());

        kafkaSourceReaderMetrics.recordSucceededCommit();

        assertEquals(
                1L,
                metricListener
                        .getCounter(
                                KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                                KafkaSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER)
                        .getCount());
    }

    @Test
    public void testNonTrackingTopicPartition() {
        MetricListener metricListener = new MetricListener();
        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(metricListener.getMetricGroup());
        assertThrows(
                IllegalArgumentException.class,
                () -> kafkaSourceReaderMetrics.recordCurrentOffset(FOO_0, 15213L));
        assertThrows(
                IllegalArgumentException.class,
                () -> kafkaSourceReaderMetrics.recordCommittedOffset(FOO_0, 15213L));
    }

    @Test
    public void testFailedCommit() {
        MetricListener metricListener = new MetricListener();
        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(metricListener.getMetricGroup());
        kafkaSourceReaderMetrics.recordFailedCommit();
        assertEquals(
                1L,
                metricListener
                        .getCounter(
                                KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                                KafkaSourceReaderMetrics.COMMITS_FAILED_METRIC_COUNTER)
                        .getCount());
    }

    // ----------- Assertions --------------

    private void assertCurrentOffset(
            TopicPartition tp, long expectedOffset, MetricListener metricListener) {
        assertGaugeValueEquals(
                expectedOffset,
                metricListener.getGauge(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        KafkaSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE),
                Long.class);
    }

    private void assertCommittedOffset(
            TopicPartition tp, long expectedOffset, MetricListener metricListener) {
        assertGaugeValueEquals(
                expectedOffset,
                metricListener.getGauge(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        KafkaSourceReaderMetrics.COMMITTED_OFFSET_METRIC_GAUGE),
                Long.class);
    }

    private <T> void assertGaugeValueEquals(T expected, Gauge<?> gauge, Class<T> type) {
        final T actual = type.cast(gauge.getValue());
        assertEquals(expected, actual);
    }

    private void assertThrows(Class<? extends Throwable> clazz, Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (!clazz.isAssignableFrom(e.getClass())) {
                throw new IllegalArgumentException(
                        "Unexpected exception thrown: " + e.getClass().getName());
            }
        }
    }
}
