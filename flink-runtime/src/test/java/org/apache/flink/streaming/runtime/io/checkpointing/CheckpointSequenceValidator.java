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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link AbstractInvokable} that validates expected order of completed and aborted checkpoints. */
class CheckpointSequenceValidator extends AbstractInvokable {

    private final long[] checkpointIDs;

    private int i = 0;

    CheckpointSequenceValidator(long... checkpointIDs) {
        super(new DummyEnvironment("test", 1, 0));
        this.checkpointIDs = checkpointIDs;
    }

    @Override
    public void invoke() {
        throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        throw new UnsupportedOperationException("should never be called");
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics) {
        assertThat(checkpointIDs)
                .as(
                        "Unexpected triggerCheckpointOnBarrier("
                                + checkpointMetaData.getCheckpointId()
                                + ")")
                .hasSizeGreaterThan(i);

        final long expectedId = checkpointIDs[i++];
        assertThat(expectedId)
                .as(
                        "got 'triggerCheckpointOnBarrier(%d)' when expecting an 'abortCheckpointOnBarrier(%d)'",
                        checkpointMetaData.getCheckpointId(), expectedId)
                .isGreaterThanOrEqualTo(0L);
        assertThat(checkpointMetaData.getCheckpointId()).isEqualTo(expectedId);
        assertThat(checkpointMetaData.getTimestamp()).isPositive();
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) {
        assertThat(checkpointIDs)
                .as("Unexpected abortCheckpointOnBarrier(" + checkpointId + ")")
                .hasSizeGreaterThan(i);

        final long expectedId = checkpointIDs[i++];
        assertThat(expectedId)
                .as(
                        "got 'abortCheckpointOnBarrier(%d)' when expecting an 'triggerCheckpointOnBarrier(%d)'",
                        checkpointId, expectedId)
                .isNegative();
        assertThat(checkpointId)
                .as("wrong checkpoint id for checkpoint abort")
                .isEqualTo(-expectedId);
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        throw new UnsupportedOperationException("should never be called");
    }
}
