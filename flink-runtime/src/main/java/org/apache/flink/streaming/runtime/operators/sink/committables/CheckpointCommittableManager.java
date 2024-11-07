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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;

import java.io.IOException;
import java.util.Collection;

/**
 * A {@code CheckpointCommittableManager} collects committables for one checkpoint across
 * potentially multiple upstream subtasks.
 *
 * <p>While it collects committables from multiple upstream subtasks, it belongs to exactly one
 * committer subtask.
 *
 * <p>Each upstream subtask of this particular checkpoint is represented by a {@link
 * SubtaskCommittableManager}.
 *
 * @param <CommT> type of the committable
 */
@Internal
public interface CheckpointCommittableManager<CommT> {
    /**
     * Returns the checkpoint id in which the committables were created.
     *
     * @return checkpoint id
     */
    long getCheckpointId();

    /** Returns the number of upstream subtasks belonging to the checkpoint. */
    int getNumberOfSubtasks();

    boolean isFinished();

    /**
     * Returns true if all committables of all upstream subtasks arrived, which is only guaranteed
     * to happen if the DOP of the caller is 1.
     */
    boolean hasGloballyReceivedAll();

    /**
     * Commits all due committables if all respective committables of the specific subtask and
     * checkpoint have been received.
     *
     * @param committer used to commit to the external system
     * @param maxRetries
     */
    void commit(Committer<CommT> committer, int maxRetries)
            throws IOException, InterruptedException;

    /**
     * Returns the number of committables that have been successfully committed; that is, the
     * corresponding {@link org.apache.flink.api.connector.sink2.Committer.CommitRequest} was not
     * used to signal an error of any kind (retryable or not).
     *
     * @return number of successful committables
     */
    Collection<CommT> getSuccessfulCommittables();

    /**
     * Returns the number of committables that have failed with a known error. By the current
     * semantics of {@link
     * org.apache.flink.api.connector.sink2.Committer.CommitRequest#signalFailedWithKnownReason(Throwable)}
     * discards the committable but proceeds processing. The returned number should be emitted
     * downstream in a {@link org.apache.flink.streaming.api.connector.sink2.CommittableSummary},
     * such that downstream can assess if all committables have been processed.
     *
     * @return number of failed committables
     */
    int getNumFailed();
}
