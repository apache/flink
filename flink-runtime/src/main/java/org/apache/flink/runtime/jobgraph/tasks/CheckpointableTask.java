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
package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.FlushEvent;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A task that participates in checkpointing.
 *
 * @see TaskInvokable
 * @see AbstractInvokable
 */
@Internal
public interface CheckpointableTask extends FlushingTask {

    /**
     * This method is called to trigger a checkpoint, asynchronously by the checkpoint coordinator.
     *
     * <p>This method is called for tasks that start the checkpoints by injecting the initial
     * barriers, i.e., the source tasks. In contrast, checkpoints on downstream operators, which are
     * the result of receiving checkpoint barriers, invoke the {@link
     * #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetricsBuilder)}
     * method.
     *
     * @param checkpointMetaData Meta data for about this checkpoint
     * @param checkpointOptions Options for performing this checkpoint
     * @return future with value of {@code false} if the checkpoint was not carried out, {@code
     *     true} otherwise
     */
    CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions);

    /**
     * This method is called when a checkpoint is triggered as a result of receiving checkpoint
     * barriers on all input streams.
     *
     * @param checkpointMetaData Meta data for about this checkpoint
     * @param checkpointOptions Options for performing this checkpoint
     * @param checkpointMetrics Metrics about this checkpoint
     * @throws IOException Exceptions thrown as the result of triggering a checkpoint are forwarded.
     */
    void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException;

    /**
     * Invoked when a checkpoint has been completed, i.e., when the checkpoint coordinator has
     * received the notification from all participating tasks.
     *
     * @param checkpointId The ID of the checkpoint that is complete.
     * @return future that completes when the notification has been processed by the task.
     */
    Future<Void> notifyCheckpointCompleteAsync(long checkpointId);

    /**
     * Invoked when a checkpoint has been aborted, i.e., when the checkpoint coordinator has
     * received a decline message from one task and try to abort the targeted checkpoint by
     * notification.
     *
     * @param checkpointId The ID of the checkpoint that is aborted.
     * @param latestCompletedCheckpointId The ID of the latest completed checkpoint.
     * @return future that completes when the notification has been processed by the task.
     */
    Future<Void> notifyCheckpointAbortAsync(long checkpointId, long latestCompletedCheckpointId);

    /**
     * Invoked when a checkpoint has been subsumed, i.e., when the checkpoint coordinator has
     * confirmed one checkpoint has been finished, and try to remove the first previous checkpoint.
     *
     * @param checkpointId The ID of the checkpoint that is subsumed.
     * @return future that completes when the notification has been processed by the task.
     */
    Future<Void> notifyCheckpointSubsumedAsync(long checkpointId);

    /**
     * Aborts a checkpoint as the result of receiving possibly some checkpoint barriers, but at
     * least one {@link org.apache.flink.runtime.io.network.api.CancelCheckpointMarker}.
     *
     * <p>This requires implementing tasks to forward a {@link
     * org.apache.flink.runtime.io.network.api.CancelCheckpointMarker} to their outputs.
     *
     * @param checkpointId The ID of the checkpoint to be aborted.
     * @param cause The reason why the checkpoint was aborted during alignment
     */
    void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause) throws IOException;


}
