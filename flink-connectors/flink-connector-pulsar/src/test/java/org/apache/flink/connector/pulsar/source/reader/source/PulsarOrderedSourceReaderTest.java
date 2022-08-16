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

package org.apache.flink.connector.pulsar.source.reader.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.extension.SubType;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.TestTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.createPartitionSplit;
import static org.apache.flink.connector.pulsar.testutils.PulsarTestCommonUtils.createPartitionSplits;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.DEFAULT_PARTITIONS;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class PulsarOrderedSourceReaderTest extends PulsarSourceReaderTestBase {

    private static final int MAX_EMPTY_POLLING_TIMES = 10;

    @SubType SubscriptionType subscriptionType = SubscriptionType.Failover;

    @TestTemplate
    void consumeMessagesAndCommitOffsets(
            PulsarSourceReaderBase<Integer> baseReader, Boundedness boundedness, String topicName)
            throws Exception {
        // set up the partition
        PulsarOrderedSourceReader<Integer> reader = (PulsarOrderedSourceReader<Integer>) baseReader;
        setupSourceReader(reader, topicName, 0, Boundedness.CONTINUOUS_UNBOUNDED);

        // waiting for results
        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        pollUntil(
                reader,
                output,
                () -> output.getEmittedRecords().size() == NUM_RECORDS_PER_PARTITION,
                "The output didn't poll enough records before timeout.");
        reader.snapshotState(100L);
        reader.notifyCheckpointComplete(100L);
        pollUntil(
                reader,
                output,
                reader.cursorsToCommit::isEmpty,
                "The offset commit did not finish before timeout.");

        // verify consumption
        reader.close();
        verifyAllMessageAcknowledged(
                NUM_RECORDS_PER_PARTITION, TopicNameUtils.topicNameWithPartition(topicName, 0));
    }

    @TestTemplate
    void offsetCommitOnCheckpointComplete(
            PulsarSourceReaderBase<Integer> baseReader, Boundedness boundedness, String topicName)
            throws Exception {
        PulsarOrderedSourceReader<Integer> reader = (PulsarOrderedSourceReader<Integer>) baseReader;
        // consume more than 1 partition
        reader.addSplits(
                createPartitionSplits(
                        topicName, DEFAULT_PARTITIONS, Boundedness.CONTINUOUS_UNBOUNDED));
        reader.notifyNoMoreSplits();
        TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
        long checkpointId = 0;
        int emptyResultTime = 0;
        InputStatus status;
        do {
            checkpointId++;
            status = reader.pollNext(output);
            // Create a checkpoint for each message consumption, but not complete them.
            reader.snapshotState(checkpointId);
            // the first couple of pollNext() might return NOTHING_AVAILABLE before data appears
            if (InputStatus.NOTHING_AVAILABLE == status) {
                emptyResultTime++;
                sleepUninterruptibly(1, TimeUnit.SECONDS);
            }

        } while (emptyResultTime < MAX_EMPTY_POLLING_TIMES
                && status != InputStatus.END_OF_INPUT
                && output.getEmittedRecords().size()
                        < NUM_RECORDS_PER_PARTITION * DEFAULT_PARTITIONS);

        // The completion of the last checkpoint should subsume all previous checkpoints.
        assertThat(reader.cursorsToCommit).hasSize((int) checkpointId);
        long lastCheckpointId = checkpointId;
        // notify checkpoint complete and expect all cursors committed
        assertThatCode(() -> reader.notifyCheckpointComplete(lastCheckpointId))
                .doesNotThrowAnyException();
        assertThat(reader.cursorsToCommit).isEmpty();

        // Verify the committed offsets.
        reader.close();
        for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
            verifyAllMessageAcknowledged(
                    NUM_RECORDS_PER_PARTITION, TopicNameUtils.topicNameWithPartition(topicName, i));
        }
    }

    private void setupSourceReader(
            PulsarSourceReaderBase<Integer> reader,
            String topicName,
            int partitionId,
            Boundedness boundedness) {
        PulsarPartitionSplit split = createPartitionSplit(topicName, partitionId, boundedness);
        reader.addSplits(Collections.singletonList(split));
        reader.notifyNoMoreSplits();
    }

    private void pollUntil(
            PulsarSourceReaderBase<Integer> reader,
            ReaderOutput<Integer> output,
            Supplier<Boolean> condition,
            String errorMessage)
            throws InterruptedException, TimeoutException {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        reader.pollNext(output);
                    } catch (Exception exception) {
                        throw new RuntimeException(
                                "Caught unexpected exception when polling from the reader",
                                exception);
                    }
                    return condition.get();
                },
                Duration.ofSeconds(Integer.MAX_VALUE),
                errorMessage);
    }

    private void verifyAllMessageAcknowledged(int expectedMessages, String partitionName)
            throws PulsarAdminException, PulsarClientException {

        Consumer<byte[]> consumer =
                operator()
                        .client()
                        .newConsumer()
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                        .subscriptionName("verify-message")
                        .topic(partitionName)
                        .subscribe();

        assertThat(((MessageIdImpl) consumer.getLastMessageId()).getEntryId())
                .isEqualTo(expectedMessages - 1);
    }
}
