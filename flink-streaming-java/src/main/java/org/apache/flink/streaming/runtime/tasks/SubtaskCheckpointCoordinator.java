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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Coordinates checkpointing-related work for a subtask (i.e. {@link
 * org.apache.flink.runtime.taskmanager.Task Task} and {@link StreamTask}). Responsibilities:
 *
 * <ol>
 *   <li>build a snapshot (invokable)
 *   <li>report snapshot to the JobManager
 *   <li>action upon checkpoint notification
 *   <li>maintain storage locations
 * </ol>
 */
@Internal
public interface SubtaskCheckpointCoordinator extends Closeable {

    /** Initialize new checkpoint. */
    void initCheckpoint(long id, CheckpointOptions checkpointOptions) throws CheckpointException;

    ChannelStateWriter getChannelStateWriter();

    CheckpointStorageWorkerView getCheckpointStorage();

    void abortCheckpointOnBarrier(
            long checkpointId, CheckpointException cause, OperatorChain<?, ?> operatorChain)
            throws IOException;

    /** Must be called after {@link #initCheckpoint(long, CheckpointOptions)}. */
    void checkpointState(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning)
            throws Exception;

    /**
     * Notified on the task side once a distributed checkpoint has been completed.
     *
     * @param checkpointId The checkpoint id to notify as been completed.
     * @param operatorChain The chain of operators executed by the task.
     * @param isRunning Whether the task is running.
     */
    void notifyCheckpointComplete(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception;

    /**
     * Notified on the task side once a distributed checkpoint has been aborted.
     *
     * @param checkpointId The checkpoint id to notify as been completed.
     * @param operatorChain The chain of operators executed by the task.
     * @param isRunning Whether the task is running.
     */
    void notifyCheckpointAborted(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception;
}
