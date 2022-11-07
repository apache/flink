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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.StopCondition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.MSG_NUM_IN_RECEIVER_QUEUE;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_ACKS_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_ACKS_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_BATCH_RECEIVE_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_BYTES_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_MSGS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.NUM_RECEIVE_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.PULSAR_CONSUMER_METRIC_NAME;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.RATE_BYTES_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.RATE_MSGS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_ACKS_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_ACKS_SENT;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_BATCH_RECEIVED_FAILED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_BYTES_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_MSGS_RECEIVED;
import static org.apache.flink.connector.pulsar.common.metrics.MetricNames.TOTAL_RECEIVED_FAILED;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.source.config.PulsarSourceConfigUtils.createConsumerBuilder;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator.KeySharedMode.JOIN;
import static org.apache.pulsar.client.api.KeySharedPolicy.stickyHashRange;

/** The common partition split reader. */
abstract class PulsarPartitionSplitReaderBase
        implements SplitReader<Message<byte[]>, PulsarPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionSplitReaderBase.class);

    protected final PulsarClient pulsarClient;
    protected final PulsarAdmin pulsarAdmin;
    protected final SourceConfiguration sourceConfiguration;
    protected final SourceReaderMetricGroup metricGroup;

    protected Consumer<byte[]> pulsarConsumer;
    protected PulsarPartitionSplit registeredSplit;

    protected PulsarPartitionSplitReaderBase(
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            SourceConfiguration sourceConfiguration,
            SourceReaderMetricGroup metricGroup) {
        this.pulsarClient = pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.sourceConfiguration = sourceConfiguration;
        this.metricGroup = metricGroup;
    }

    @Override
    public RecordsWithSplitIds<Message<byte[]>> fetch() throws IOException {
        RecordsBySplits.Builder<Message<byte[]>> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (pulsarConsumer == null || registeredSplit == null) {
            return builder.build();
        }

        StopCursor stopCursor = registeredSplit.getStopCursor();
        String splitId = registeredSplit.splitId();
        Deadline deadline = Deadline.fromNow(sourceConfiguration.getMaxFetchTime());

        // Consume messages from pulsar until it was woken up by flink reader.
        for (int messageNum = 0;
                messageNum < sourceConfiguration.getMaxFetchRecords() && deadline.hasTimeLeft();
                messageNum++) {
            try {
                Duration timeout = deadline.timeLeftIfAny();
                Message<byte[]> message = pollMessage(timeout);
                if (message == null) {
                    break;
                }

                StopCondition condition = stopCursor.shouldStop(message);

                if (condition == StopCondition.CONTINUE || condition == StopCondition.EXACTLY) {
                    // Collect original message.
                    builder.add(splitId, message);
                    // Acknowledge the message if you need.
                    finishedPollMessage(message);
                }

                if (condition == StopCondition.EXACTLY || condition == StopCondition.TERMINATE) {
                    builder.addFinishedSplit(splitId);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (TimeoutException e) {
                break;
            } catch (ExecutionException e) {
                LOG.error("Error in polling message from pulsar consumer.", e);
                break;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PulsarPartitionSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        // Get all the partition assignments and stopping offsets.
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (registeredSplit != null) {
            throw new IllegalStateException("This split reader have assigned split.");
        }

        List<PulsarPartitionSplit> newSplits = splitsChanges.splits();
        Preconditions.checkArgument(
                newSplits.size() == 1, "This pulsar split reader only supports one split.");
        this.registeredSplit = newSplits.get(0);

        // Open stop cursor.
        registeredSplit.open(pulsarAdmin);

        // Before creating the consumer.
        beforeCreatingConsumer(registeredSplit);

        // Create pulsar consumer.
        this.pulsarConsumer = createPulsarConsumer(registeredSplit);

        // After creating the consumer.
        afterCreatingConsumer(registeredSplit, pulsarConsumer);

        LOG.info("Register split {} consumer for current reader.", registeredSplit);
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<PulsarPartitionSplit> splitsToPause,
            Collection<PulsarPartitionSplit> splitsToResume) {
        // This shouldn't happen but just in case...
        Preconditions.checkState(
                splitsToPause.size() + splitsToResume.size() <= 1,
                "This pulsar split reader only supports one split.");

        if (!splitsToPause.isEmpty()) {
            pulsarConsumer.pause();
        } else if (!splitsToResume.isEmpty()) {
            pulsarConsumer.resume();
        }
    }

    @Override
    public void wakeUp() {
        // Nothing to do on this method.
    }

    @Override
    public void close() {
        if (pulsarConsumer != null) {
            sneakyClient(() -> pulsarConsumer.close());
        }
    }

    @Nullable
    protected abstract Message<byte[]> pollMessage(Duration timeout)
            throws ExecutionException, InterruptedException, PulsarClientException;

    protected abstract void finishedPollMessage(Message<byte[]> message);

    protected void beforeCreatingConsumer(PulsarPartitionSplit split) {
        // Nothing to do by default.
    }

    protected void afterCreatingConsumer(PulsarPartitionSplit split, Consumer<byte[]> consumer) {
        // Nothing to do by default.
    }

    // --------------------------- Helper Methods -----------------------------

    /** Create a specified {@link Consumer} by the given split information. */
    protected Consumer<byte[]> createPulsarConsumer(PulsarPartitionSplit split) {
        return createPulsarConsumer(split.getPartition());
    }

    /** Create a specified {@link Consumer} by the given topic partition. */
    protected Consumer<byte[]> createPulsarConsumer(TopicPartition partition) {
        ConsumerBuilder<byte[]> consumerBuilder =
                createConsumerBuilder(pulsarClient, Schema.BYTES, sourceConfiguration);

        consumerBuilder.topic(partition.getFullTopicName());

        // Add KeySharedPolicy for Key_Shared subscription.
        if (sourceConfiguration.getSubscriptionType() == SubscriptionType.Key_Shared) {
            KeySharedPolicy policy = stickyHashRange().ranges(partition.getPulsarRanges());
            // We may enable out of order delivery for speeding up. It was turned off by default.
            policy.setAllowOutOfOrderDelivery(
                    sourceConfiguration.isAllowKeySharedOutOfOrderDelivery());
            consumerBuilder.keySharedPolicy(policy);

            if (partition.getMode() == JOIN) {
                // Override the key shared subscription into exclusive for making it behaviors like
                // a Pulsar Reader which supports partial key hash ranges.
                consumerBuilder.subscriptionType(SubscriptionType.Exclusive);
            }
        }

        // Create the consumer configuration by using common utils.
        Consumer<byte[]> consumer = sneakyClient(consumerBuilder::subscribe);

        // Exposing the consumer metrics.
        exposeConsumerMetrics(consumer);

        return consumer;
    }

    private void exposeConsumerMetrics(Consumer<byte[]> consumer) {
        if (sourceConfiguration.isEnableMetrics()) {
            String consumerIdentity = consumer.getConsumerName();
            if (Strings.isNullOrEmpty(consumerIdentity)) {
                consumerIdentity = UUID.randomUUID().toString();
            }

            MetricGroup group =
                    metricGroup
                            .addGroup(PULSAR_CONSUMER_METRIC_NAME)
                            .addGroup(consumer.getTopic())
                            .addGroup(consumerIdentity);
            ConsumerStats stats = consumer.getStats();

            group.gauge(NUM_MSGS_RECEIVED, stats::getNumMsgsReceived);
            group.gauge(NUM_BYTES_RECEIVED, stats::getNumBytesReceived);
            group.gauge(RATE_MSGS_RECEIVED, stats::getRateMsgsReceived);
            group.gauge(RATE_BYTES_RECEIVED, stats::getRateBytesReceived);
            group.gauge(NUM_ACKS_SENT, stats::getNumAcksSent);
            group.gauge(NUM_ACKS_FAILED, stats::getNumAcksFailed);
            group.gauge(NUM_RECEIVE_FAILED, stats::getNumReceiveFailed);
            group.gauge(NUM_BATCH_RECEIVE_FAILED, stats::getNumBatchReceiveFailed);
            group.gauge(TOTAL_MSGS_RECEIVED, stats::getTotalMsgsReceived);
            group.gauge(TOTAL_BYTES_RECEIVED, stats::getTotalBytesReceived);
            group.gauge(TOTAL_RECEIVED_FAILED, stats::getTotalReceivedFailed);
            group.gauge(TOTAL_BATCH_RECEIVED_FAILED, stats::getTotaBatchReceivedFailed);
            group.gauge(TOTAL_ACKS_SENT, stats::getTotalAcksSent);
            group.gauge(TOTAL_ACKS_FAILED, stats::getTotalAcksFailed);
            group.gauge(MSG_NUM_IN_RECEIVER_QUEUE, stats::getMsgNumInReceiverQueue);
        }
    }
}
