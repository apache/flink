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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.execution.ExecutionState;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@code StopWithSavepointTerminationHandler} handles the termination steps necessary for the
 * stop-with-savepoint operation to finish. The order of the terminations matter:
 *
 * <ol>
 *   <li>Creating a savepoint needs to be completed
 *   <li>Waiting for the executions of the underlying job to finish
 * </ol>
 */
public interface StopWithSavepointTerminationHandler {

    /**
     * Returns the a {@code CompletableFuture} referring to the result of the stop-with-savepoint
     * operation.
     *
     * @return the {@code CompletableFuture} containing the path to the created savepoint in case of
     *     success.
     */
    CompletableFuture<String> getSavepointPath();

    /**
     * Handles the result of a {@code CompletableFuture} holding a {@link CompletedCheckpoint}. Only
     * one of the two parameters are allowed to be set.
     *
     * @param completedSavepoint the {@code CompletedCheckpoint} referring to the created savepoint
     * @param throwable an error that was caught during savepoint creation
     * @throws IllegalArgumentException if {@code throwable} and {@code completedSavepoint} are set
     * @throws NullPointerException if none of the parameters is set
     */
    void handleSavepointCreation(
            @Nullable CompletedCheckpoint completedSavepoint, @Nullable Throwable throwable);

    /**
     * Handles the termination of the job based on the passed terminated {@link ExecutionState
     * ExecutionStates}. stop-with-savepoint expects the {@code terminatedExecutionStates} to only
     * contain {@link ExecutionState#FINISHED} to succeed.
     *
     * @param terminatedExecutionStates The terminated {@code ExecutionStates} of the underlying
     *     job.
     * @throws NullPointerException if {@code null} is passed.
     */
    void handleExecutionsTermination(Collection<ExecutionState> terminatedExecutionStates);
}
