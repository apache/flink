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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.EmitsRecordsOnFinalCheckpoint;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CompletingCheckpointResponder;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a task whose operator chain emits records on the final checkpoint's {@code
 * notifyCheckpointComplete} (see {@link EmitsRecordsOnFinalCheckpoint}) defers broadcasting {@code
 * EndOfData} downstream until those records have been emitted (FLINK-38614).
 *
 * <p>The sink {@code CommitterOperator} forwards the final checkpoint's committables to a
 * downstream (global) committer only on {@code notifyCheckpointComplete}, i.e. after {@code
 * endInput}. Without the deferral, the task broadcasts {@code EndOfData} right after {@code
 * endInput} (before that final notification) and the downstream committer finishes before receiving
 * the committables, silently dropping them.
 */
class StreamTaskDeferredEndOfDataTest {

    private static final String MARKER = "emitted-on-final-checkpoint";

    @Test
    void testEndOfDataDeferredUntilFinalCheckpointRecordsAreEmitted() throws Exception {
        CompletingCheckpointResponder checkpointResponder = new CompletingCheckpointResponder();
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 1)
                        .setCollectNetworkEvents()
                        .addJobConfig(
                                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1))
                        .setCheckpointResponder(checkpointResponder)
                        .setupOperatorChain(new EmitOnFinalCheckpointOperator())
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {
            checkpointResponder.setHandlers(
                    harness.streamTask::notifyCheckpointCompleteAsync,
                    harness.streamTask::notifyCheckpointAbortAsync);

            // Exhaust the input. This drives endData/finishOperators; with the deferral the
            // downstream EndOfData broadcast is held back.
            harness.processEvent(new EndOfData(StopMode.DRAIN), 0, 0);
            harness.processAll();

            // EndOfData must not have been broadcast yet: the operator has not emitted its
            // final-checkpoint records.
            assertThat(harness.getOutput()).doesNotContain(new EndOfData(StopMode.DRAIN));

            // The final checkpoint completes; the operator emits its record on
            // notifyCheckpointComplete and only then is the deferred EndOfData broadcast.
            CompletableFuture<Boolean> checkpointFuture = triggerCheckpoint(harness, 2L);
            processMailTillCheckpointSucceeds(harness, checkpointFuture);
            // Deliver the final checkpoint notification deterministically (rather than relying on
            // the responder's asynchronous callback): the operator emits its record here and the
            // deferred EndOfData is then flushed.
            harness.streamTask.notifyCheckpointCompleteAsync(2L);
            harness.processAll();

            // The record emitted on the final checkpoint must precede EndOfData downstream.
            assertThat(harness.getOutput())
                    .containsSubsequence(new StreamRecord<>(MARKER), new EndOfData(StopMode.DRAIN));
        }
    }

    @Test
    void testDeferredEndOfDataNotFlushedUntilFinalCheckpointCompletes() throws Exception {
        CompletingCheckpointResponder checkpointResponder = new CompletingCheckpointResponder();
        // Only the second post-end-of-input checkpoint completes; the first is never notified.
        checkpointResponder.completeCheckpoints(Collections.singletonList(3L));
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                OneInputStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO)
                        .addInput(BasicTypeInfo.STRING_TYPE_INFO, 1)
                        .setCollectNetworkEvents()
                        .addJobConfig(
                                CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1))
                        .setCheckpointResponder(checkpointResponder)
                        .setupOperatorChain(new EmitOnFinalCheckpointOperator())
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {
            checkpointResponder.setHandlers(
                    harness.streamTask::notifyCheckpointCompleteAsync,
                    harness.streamTask::notifyCheckpointAbortAsync);

            harness.processEvent(new EndOfData(StopMode.DRAIN), 0, 0);
            harness.processAll();

            // The first post-end-of-input checkpoint is triggered but never completes; EndOfData
            // and the final-checkpoint record must both stay deferred.
            triggerCheckpoint(harness, 2L);
            harness.processAll();
            assertThat(harness.getOutput())
                    .doesNotContain(new EndOfData(StopMode.DRAIN), new StreamRecord<>(MARKER));

            // A later checkpoint completes; only then are the record and the deferred EndOfData
            // emitted, in that order.
            CompletableFuture<Boolean> completed = triggerCheckpoint(harness, 3L);
            processMailTillCheckpointSucceeds(harness, completed);
            // Deliver cp3's notification deterministically; the operator emits its record and the
            // deferred EndOfData is then flushed (cp2 was never notified, so nothing flushed
            // above).
            harness.streamTask.notifyCheckpointCompleteAsync(3L);
            harness.processAll();
            assertThat(harness.getOutput())
                    .containsSubsequence(new StreamRecord<>(MARKER), new EndOfData(StopMode.DRAIN));
        }
    }

    private static CompletableFuture<Boolean> triggerCheckpoint(
            StreamTaskMailboxTestHarness<String> harness, long checkpointId) {
        harness.getTaskStateManager().getWaitForReportLatch().reset();
        return harness.getStreamTask()
                .triggerCheckpointAsync(
                        new CheckpointMetaData(checkpointId, checkpointId * 1000),
                        CheckpointOptions.forCheckpointWithDefaultLocation());
    }

    private static void processMailTillCheckpointSucceeds(
            StreamTaskMailboxTestHarness<String> harness, Future<Boolean> checkpointFuture)
            throws Exception {
        while (!checkpointFuture.isDone()) {
            harness.processSingleStep();
        }
        harness.getTaskStateManager().getWaitForReportLatch().await();
    }

    /**
     * Operator that emits a record on the final checkpoint's {@code notifyCheckpointComplete},
     * mirroring how {@code CommitterOperator} forwards committables downstream.
     */
    private static class EmitOnFinalCheckpointOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>, EmitsRecordsOnFinalCheckpoint {

        @Override
        public void processElement(StreamRecord<String> element) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            super.notifyCheckpointComplete(checkpointId);
            output.collect(new StreamRecord<>(MARKER));
        }

        @Override
        public boolean emitsRecordsOnFinalCheckpoint() {
            return true;
        }
    }
}
