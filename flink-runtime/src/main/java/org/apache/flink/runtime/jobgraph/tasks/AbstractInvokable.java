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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base implementation of {@link TaskInvokable}, {@link CheckpointableTask}, and {@link
 * CoordinatedTask} with most methods throwing {@link UnsupportedOperationException} or doing
 * nothing.
 *
 * <p>Any subclass that supports recoverable state and participates in checkpointing needs to
 * override the methods of {@link CheckpointableTask}, such as {@link
 * #triggerCheckpointAsync(CheckpointMetaData, CheckpointOptions)}, {@link
 * #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetricsBuilder)},
 * {@link #abortCheckpointOnBarrier(long, CheckpointException)} and {@link
 * #notifyCheckpointCompleteAsync(long)}.
 */
public abstract class AbstractInvokable
        implements TaskInvokable, CheckpointableTask, CoordinatedTask {

    /** The environment assigned to this invokable. */
    private final Environment environment;

    /**
     * Create an Invokable task and set its environment.
     *
     * @param environment The environment assigned to this invokable.
     */
    public AbstractInvokable(Environment environment) {
        this.environment = checkNotNull(environment);
    }

    // ------------------------------------------------------------------------
    //  Core methods
    // ------------------------------------------------------------------------

    @Override
    public abstract void invoke() throws Exception;

    @Override
    public Future<Void> cancel() throws Exception {
        // The default implementation does nothing.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void cleanUp(@Nullable Throwable throwable) throws Exception {}

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        if (taskName != null && timeout != null) {
            Task.logTaskThreadStackTrace(toInterrupt, taskName, timeout, "interrupting");
        }

        toInterrupt.interrupt();
    }

    // ------------------------------------------------------------------------
    //  Access to Environment and Configuration
    // ------------------------------------------------------------------------

    /**
     * Returns the environment of this task.
     *
     * @return The environment of this task.
     */
    public final Environment getEnvironment() {
        return this.environment;
    }

    /**
     * Returns the user code class loader of this invokable.
     *
     * @return user code class loader of this invokable.
     */
    public final ClassLoader getUserCodeClassLoader() {
        return getEnvironment().getUserCodeClassLoader().asClassLoader();
    }

    /**
     * Returns the current number of subtasks the respective task is split into.
     *
     * @return the current number of subtasks the respective task is split into
     */
    public int getCurrentNumberOfSubtasks() {
        return this.environment.getTaskInfo().getNumberOfParallelSubtasks();
    }

    /**
     * Returns the index of this subtask in the subtask group.
     *
     * @return the index of this subtask in the subtask group
     */
    public int getIndexInSubtaskGroup() {
        return this.environment.getTaskInfo().getIndexOfThisSubtask();
    }

    /**
     * Returns the task configuration object which was attached to the original {@link
     * org.apache.flink.runtime.jobgraph.JobVertex}.
     *
     * @return the task configuration object which was attached to the original {@link
     *     org.apache.flink.runtime.jobgraph.JobVertex}
     */
    public final Configuration getTaskConfiguration() {
        return this.environment.getTaskConfiguration();
    }

    /**
     * Returns the job configuration object which was attached to the original {@link
     * org.apache.flink.runtime.jobgraph.JobGraph}.
     *
     * @return the job configuration object which was attached to the original {@link
     *     org.apache.flink.runtime.jobgraph.JobGraph}
     */
    public Configuration getJobConfiguration() {
        return this.environment.getJobConfiguration();
    }

    /** Returns the global ExecutionConfig. */
    public ExecutionConfig getExecutionConfig() {
        return this.environment.getExecutionConfig();
    }

    // ------------------------------------------------------------------------
    //  Checkpointing Methods
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        throw new UnsupportedOperationException(
                String.format(
                        "triggerCheckpointAsync not supported by %s", this.getClass().getName()));
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {
        throw new UnsupportedOperationException(
                String.format(
                        "triggerCheckpointOnBarrier not supported by %s",
                        this.getClass().getName()));
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        throw new UnsupportedOperationException(
                String.format(
                        "abortCheckpointOnBarrier not supported by %s", this.getClass().getName()));
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        throw new UnsupportedOperationException(
                String.format(
                        "notifyCheckpointCompleteAsync not supported by %s",
                        this.getClass().getName()));
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        throw new UnsupportedOperationException(
                String.format(
                        "notifyCheckpointAbortAsync not supported by %s",
                        this.getClass().getName()));
    }

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        throw new UnsupportedOperationException(
                "dispatchOperatorEvent not supported by " + getClass().getName());
    }

    @Override
    public void restore() throws Exception {}

    @Override
    public boolean isUsingNonBlockingInput() {
        return false;
    }
}
