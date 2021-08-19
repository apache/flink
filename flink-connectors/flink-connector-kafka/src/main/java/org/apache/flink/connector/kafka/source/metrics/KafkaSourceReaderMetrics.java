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

import org.apache.flink.connector.kafka.source.reader.KafkaSourceReader;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A collection class for handling metrics in {@link KafkaSourceReader}.
 *
 * <p>All metrics of Kafka source reader are registered under group "KafkaSourceReader", which is a
 * child group of {@link org.apache.flink.runtime.metrics.groups.OperatorMetricGroup}. Metrics
 * related to a specific topic partition will be registered in the group
 * "KafkaSourceReader.topic.{topic_name}.partition.{partition_id}".
 *
 * <p>For example, current consuming offset of topic "my-topic" and partition 1 will be reported in
 * metric:
 * "{some_parent_groups}.operator.KafkaSourceReader.topic.my-topic.partition.1.currentOffset"
 *
 * <p>and number of successful commits will be reported in metric:
 * "{some_parent_groups}.operator.KafkaSourceReader.commitsSucceeded"
 *
 * <p>All metrics of Kafka consumer are also registered under group
 * "KafkaSourceReader.KafkaConsumer". For example, Kafka consumer metric "records-consumed-total"
 * can be found at:
 * {some_parent_groups}.operator.KafkaSourceReader.KafkaConsumer.records-consumed-total"
 */
public class KafkaSourceReaderMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceReaderMetrics.class);

    // Constants
    public static final String KAFKA_SOURCE_READER_METRIC_GROUP = "KafkaSourceReader";
    public static final String TOPIC_GROUP = "topic";
    public static final String PARTITION_GROUP = "partition";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";
    public static final String COMMITTED_OFFSET_METRIC_GAUGE = "committedOffset";
    public static final String COMMITS_SUCCEEDED_METRIC_COUNTER = "commitsSucceeded";
    public static final String COMMITS_FAILED_METRIC_COUNTER = "commitsFailed";
    public static final String KAFKA_CONSUMER_METRIC_GROUP = "KafkaConsumer";

    public static final long INITIAL_OFFSET = -1;

    // Metric group for registering Kafka related metrics
    private final MetricGroup kafkaSourceReaderMetricGroup;

    // Successful / Failed commits counters
    private final Counter commitsSucceeded;
    private final Counter commitsFailed;

    // Map for tracking current consuming / committing offsets
    private final Map<TopicPartition, Offset> offsets = new HashMap<>();

    public KafkaSourceReaderMetrics(MetricGroup parentMetricGroup) {
        this.kafkaSourceReaderMetricGroup =
                parentMetricGroup.addGroup(KAFKA_SOURCE_READER_METRIC_GROUP);
        this.commitsSucceeded =
                this.kafkaSourceReaderMetricGroup.counter(COMMITS_SUCCEEDED_METRIC_COUNTER);
        this.commitsFailed =
                this.kafkaSourceReaderMetricGroup.counter(COMMITS_FAILED_METRIC_COUNTER);
    }

    /**
     * Register metrics of KafkaConsumer in Kafka metric group.
     *
     * @param kafkaConsumer Kafka consumer used by partition split reader.
     */
    @SuppressWarnings("Convert2MethodRef")
    public void registerKafkaConsumerMetrics(KafkaConsumer<?, ?> kafkaConsumer) {
        final Map<MetricName, ? extends Metric> kafkaConsumerMetrics = kafkaConsumer.metrics();
        if (kafkaConsumerMetrics == null) {
            LOG.warn("Consumer implementation does not support metrics");
            return;
        }

        final MetricGroup kafkaConsumerMetricGroup =
                kafkaSourceReaderMetricGroup.addGroup(KAFKA_CONSUMER_METRIC_GROUP);

        kafkaConsumerMetrics.forEach(
                (name, metric) ->
                        kafkaConsumerMetricGroup.gauge(name.name(), () -> metric.metricValue()));
    }

    /**
     * Register metric groups for the given {@link TopicPartition}.
     *
     * @param tp Registering topic partition
     */
    public void registerTopicPartition(TopicPartition tp) {
        offsets.put(tp, new Offset(INITIAL_OFFSET, INITIAL_OFFSET));
        registerOffsetMetricsForTopicPartition(tp);
    }

    /**
     * Update current consuming offset of the given {@link TopicPartition}.
     *
     * @param tp Updating topic partition
     * @param offset Current consuming offset
     */
    public void recordCurrentOffset(TopicPartition tp, long offset) {
        checkTopicPartitionTracked(tp);
        offsets.get(tp).currentOffset = offset;
    }

    /**
     * Update the latest committed offset of the given {@link TopicPartition}.
     *
     * @param tp Updating topic partition
     * @param offset Committing offset
     */
    public void recordCommittedOffset(TopicPartition tp, long offset) {
        checkTopicPartitionTracked(tp);
        offsets.get(tp).committedOffset = offset;
    }

    /** Mark a successful commit. */
    public void recordSucceededCommit() {
        commitsSucceeded.inc();
    }

    /** Mark a failure commit. */
    public void recordFailedCommit() {
        commitsFailed.inc();
    }

    // -------- Helper functions --------
    private void registerOffsetMetricsForTopicPartition(TopicPartition tp) {
        final MetricGroup topicPartitionGroup =
                this.kafkaSourceReaderMetricGroup
                        .addGroup(TOPIC_GROUP, tp.topic())
                        .addGroup(PARTITION_GROUP, String.valueOf(tp.partition()));
        topicPartitionGroup.gauge(
                CURRENT_OFFSET_METRIC_GAUGE,
                () ->
                        offsets.getOrDefault(tp, new Offset(INITIAL_OFFSET, INITIAL_OFFSET))
                                .currentOffset);
        topicPartitionGroup.gauge(
                COMMITTED_OFFSET_METRIC_GAUGE,
                () ->
                        offsets.getOrDefault(tp, new Offset(INITIAL_OFFSET, INITIAL_OFFSET))
                                .committedOffset);
    }

    private void checkTopicPartitionTracked(TopicPartition tp) {
        if (!offsets.containsKey(tp)) {
            throw new IllegalArgumentException(
                    String.format("TopicPartition %s is not tracked", tp));
        }
    }

    private static class Offset {
        long currentOffset;
        long committedOffset;

        Offset(long currentOffset, long committedOffset) {
            this.currentOffset = currentOffset;
            this.committedOffset = committedOffset;
        }
    }
}
