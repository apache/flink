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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.RegionalCheckpointInfo;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This context is the interface through which the {@link CheckpointCoordinator} interacts with an
 * {@link OperatorCoordinator} during checkpointing and checkpoint restoring.
 */
public interface OperatorCoordinatorCheckpointContext extends OperatorInfo, CheckpointListener {

    void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception;

    void abortCurrentTriggering();

    /**
     * We override the method here to remove the checked exception. Please check the Java docs of
     * {@link CheckpointListener#notifyCheckpointComplete(long)} for more detail semantic of the
     * method.
     */
    @Override
    void notifyCheckpointComplete(long checkpointId);

    /**
     * Notifies the coordinator that a regional checkpoint has completed, providing context about
     * which subtasks used historical state. Default implementation delegates to {@link
     * #notifyCheckpointComplete(long)}.
     */
    @Override
    default void notifyRegionalCheckpointComplete(
            long checkpointId, RegionalCheckpointInfo regionalCheckpointInfo) throws Exception {
        notifyCheckpointComplete(checkpointId);
    }

    /**
     * Notifies the coordinator that a regional checkpoint has completed but some subtasks fell back
     * to a historical checkpoint. Default is no-op; coordinators that maintain local state should
     * override to clean up stale state for the fallback subtasks.
     */
    @Override
    default void notifyRegionalCheckpointFallback(long checkpointId, long fallbackCheckpointId) {
        // no-op for backward compatibility
    }

    /**
     * We override the method here to remove the checked exception. Please check the Java docs of
     * {@link CheckpointListener#notifyCheckpointAborted(long)} for more detail semantic of the
     * method.
     */
    @Override
    default void notifyCheckpointAborted(long checkpointId) {}

    /**
     * Resets the coordinator to the checkpoint with the given state.
     *
     * <p>This method is called with a null state argument in the following situations:
     *
     * <ul>
     *   <li>There is a recovery and there was no completed checkpoint yet.
     *   <li>There is a recovery from a completed checkpoint/savepoint but it contained no state for
     *       the coordinator.
     * </ul>
     *
     * <p>In both cases, the coordinator should reset to an empty (new) state.
     */
    void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception;

    /**
     * Called if a task is recovered as part of a <i>partial failover</i>, meaning a failover
     * handled by the scheduler's failover strategy (by default recovering a pipelined region). The
     * method is invoked for each subtask involved in that partial failover.
     *
     * <p>In contrast to this method, the {@link #resetToCheckpoint(long, byte[])} method is called
     * in the case of a global failover, which is the case when the coordinator (JobManager) is
     * recovered.
     */
    void subtaskReset(int subtask, long checkpointId);

    /**
     * Returns whether this coordinator supports regional checkpoints. When {@code true}, the
     * framework may call {@link #checkpointCoordinatorForRegionFallback} instead of aborting the
     * checkpoint when some subtasks decline.
     */
    default boolean supportsRegionCheckpoint() {
        return false;
    }

    /**
     * Takes a region-aware snapshot of the coordinator. Called when a regional checkpoint is being
     * completed and some subtasks' state will fall back to a previous checkpoint.
     *
     * @param checkpointId the id of the ongoing checkpoint
     * @param fallbackCheckpointId the id of the previous checkpoint for fallback subtasks
     * @param fallbackSubtaskIds subtask indices whose state will be replaced
     * @param resultFuture future to complete with the serialized coordinator state
     */
    default void checkpointCoordinatorForRegionFallback(
            long checkpointId,
            long fallbackCheckpointId,
            Set<Integer> fallbackSubtaskIds,
            CompletableFuture<byte[]> resultFuture)
            throws Exception {
        throw new UnsupportedOperationException(
                "This coordinator does not support region checkpoints.");
    }
}
