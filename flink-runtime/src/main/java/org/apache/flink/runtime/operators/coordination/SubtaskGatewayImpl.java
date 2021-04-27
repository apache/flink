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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Implementation of the {@link OperatorCoordinator.SubtaskGateway} interface that access to
 * subtasks for status and event sending via {@link SubtaskAccess}.
 */
class SubtaskGatewayImpl implements OperatorCoordinator.SubtaskGateway {

    private static final String EVENT_LOSS_ERROR_MESSAGE =
            "An OperatorEvent from an OperatorCoordinator to a task was lost. "
                    + "Triggering task failover to ensure consistency. Event: '%s', targetTask: %s";

    private final SubtaskAccess subtaskAccess;
    private final EventSender sender;
    private final Executor sendingExecutor;

    SubtaskGatewayImpl(SubtaskAccess subtaskAccess, EventSender sender, Executor sendingExecutor) {
        this.subtaskAccess = subtaskAccess;
        this.sender = sender;
        this.sendingExecutor = sendingExecutor;
    }

    @Override
    public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
        if (!isReady()) {
            throw new FlinkRuntimeException("SubtaskGateway is not ready, task not yet running.");
        }

        final SerializedValue<OperatorEvent> serializedEvent;
        try {
            serializedEvent = new SerializedValue<>(evt);
        } catch (IOException e) {
            // we do not expect that this exception is handled by the caller, so we make it
            // unchecked so that it can bubble up
            throw new FlinkRuntimeException("Cannot serialize operator event", e);
        }

        final Callable<CompletableFuture<Acknowledge>> sendAction =
                subtaskAccess.createEventSendAction(serializedEvent);

        final CompletableFuture<Acknowledge> result = new CompletableFuture<>();
        FutureUtils.assertNoException(
                result.handleAsync(
                        (success, failure) -> {
                            if (failure != null && subtaskAccess.isStillRunning()) {
                                String msg =
                                        String.format(
                                                EVENT_LOSS_ERROR_MESSAGE,
                                                evt,
                                                subtaskAccess.subtaskName());
                                subtaskAccess.triggerTaskFailover(new FlinkException(msg, failure));
                            }
                            return null;
                        },
                        sendingExecutor));

        sendingExecutor.execute(() -> sender.sendEvent(sendAction, result));
        return result;
    }

    @Override
    public ExecutionAttemptID getExecution() {
        return subtaskAccess.currentAttempt();
    }

    @Override
    public int getSubtask() {
        return subtaskAccess.getSubtaskIndex();
    }

    private boolean isReady() {
        return subtaskAccess.hasSwitchedToRunning().isDone();
    }
}
