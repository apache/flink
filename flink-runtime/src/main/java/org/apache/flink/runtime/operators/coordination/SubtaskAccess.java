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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * This interface offers access to a parallel subtask in the scope of the subtask as the target for
 * sending {@link OperatorEvent}s from an {@link OperatorCoordinator}.
 *
 * <p><b>Important:</b> An instance of this access must be bound to one specific execution attempt
 * of the subtask. After that execution attempt failed, that instance must not bind to another
 * execution attempt, but a new instance would need to be created via the {@link
 * SubtaskAccess.SubtaskAccessFactory}.
 */
interface SubtaskAccess {

    /**
     * Creates a Callable that, when invoked, sends the event to the execution attempt of the
     * subtask that this {@code SubtaskAccess} instance binds to. The resulting future from the
     * sending is returned by the callable.
     *
     * <p>This let's the caller target the specific subtask without necessarily sending the event
     * now (for example, the event may be sent at a later point due to checkpoint alignment through
     * the {@link OperatorEventValve}).
     */
    Callable<CompletableFuture<Acknowledge>> createEventSendAction(
            SerializedValue<OperatorEvent> event);

    /** Gets the parallel subtask index of the target subtask. */
    int getSubtaskIndex();

    /** Gets the execution attempt ID of the attempt that this instance is bound to. */
    ExecutionAttemptID currentAttempt();

    /**
     * Gets a descriptive name of the operator's subtask , including name, subtask-id, parallelism,
     * and execution attempt.
     */
    String subtaskName();

    /**
     * The future returned here completes once the target subtask is in a running state. As running
     * state classify the states {@link ExecutionState#RUNNING} and {@link
     * ExecutionState#INITIALIZING}.
     */
    CompletableFuture<?> hasSwitchedToRunning();

    /**
     * Checks whether the execution is still in a running state. See {@link #hasSwitchedToRunning()}
     * for details.
     */
    boolean isStillRunning();

    /**
     * Triggers a failover for the subtaks execution attempt that this access instance is bound to.
     */
    void triggerTaskFailover(Throwable cause);

    // ------------------------------------------------------------------------

    /**
     * While the {@link SubtaskAccess} is bound to an execution attempt of a subtask (like an {@link
     * org.apache.flink.runtime.executiongraph.Execution}, this factory is bound to the operator as
     * a whole (like in the scope of an {@link
     * org.apache.flink.runtime.executiongraph.ExecutionJobVertex}.
     */
    interface SubtaskAccessFactory {

        /**
         * Creates an access to the current execution attempt of the subtask with the given
         * subtaskIndex.
         */
        SubtaskAccess getAccessForSubtask(int subtaskIndex);
    }
}
