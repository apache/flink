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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.runtime.util.Runnables;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the {@link OperatorCoordinator.SubtaskGateway} interface that access to
 * subtasks for status and event sending via {@link SubtaskAccess}.
 *
 * <p>Instances of this class can be temporarily closed, blocking events from going through,
 * buffering them, and releasing them later. It is used for "alignment" of operator event streams
 * with checkpoint barrier injection, similar to how the input channels are aligned during a common
 * checkpoint.
 *
 * <p>The methods on the critical communication path, including closing/reopening the gateway and
 * sending the operator events, are required to be used in a single-threaded context specified by
 * the {@link #mainThreadExecutor} in {@link #SubtaskGatewayImpl(SubtaskAccess,
 * ComponentMainThreadExecutor, IncompleteFuturesTracker)} in order to avoid race condition.
 */
class SubtaskGatewayImpl implements OperatorCoordinator.SubtaskGateway {

    private static final String EVENT_LOSS_ERROR_MESSAGE =
            "An OperatorEvent from an OperatorCoordinator to a task was lost. "
                    + "Triggering task failover to ensure consistency. Event: '%s', targetTask: %s";

    private static final long NO_CHECKPOINT = Long.MIN_VALUE;

    private final SubtaskAccess subtaskAccess;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    private final IncompleteFuturesTracker incompleteFuturesTracker;

    private final List<BlockedEvent> blockedEvents;

    private long currentCheckpointId;

    private long lastCheckpointId;

    private boolean isClosed;

    SubtaskGatewayImpl(
            SubtaskAccess subtaskAccess,
            ComponentMainThreadExecutor mainThreadExecutor,
            IncompleteFuturesTracker incompleteFuturesTracker) {
        this.subtaskAccess = subtaskAccess;
        this.mainThreadExecutor = mainThreadExecutor;
        this.incompleteFuturesTracker = incompleteFuturesTracker;
        this.blockedEvents = new ArrayList<>();
        this.currentCheckpointId = NO_CHECKPOINT;
        this.lastCheckpointId = Long.MIN_VALUE;
        this.isClosed = false;
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

        final CompletableFuture<Acknowledge> sendResult = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> result =
                sendResult.whenCompleteAsync(
                        (success, failure) -> {
                            if (failure != null && subtaskAccess.isStillRunning()) {
                                String msg =
                                        String.format(
                                                EVENT_LOSS_ERROR_MESSAGE,
                                                evt,
                                                subtaskAccess.subtaskName());
                                Runnables.assertNoException(
                                        () ->
                                                subtaskAccess.triggerTaskFailover(
                                                        new FlinkException(msg, failure)));
                            }
                        },
                        mainThreadExecutor);

        mainThreadExecutor.execute(
                () -> {
                    sendEventInternal(sendAction, sendResult);
                    incompleteFuturesTracker.trackFutureWhileIncomplete(result);
                });

        return result;
    }

    private void sendEventInternal(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        checkRunsInMainThread();

        if (isClosed) {
            blockedEvents.add(new BlockedEvent(sendAction, result));
        } else {
            callSendAction(sendAction, result);
        }
    }

    private void callSendAction(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        try {
            final CompletableFuture<Acknowledge> sendResult = sendAction.call();
            FutureUtils.forward(sendResult, result);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalError(t);
            result.completeExceptionally(t);
        }
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

    /**
     * Marks the gateway for the next checkpoint. This remembers the checkpoint ID and will only
     * allow closing the gateway for this specific checkpoint.
     *
     * <p>This is the gateway's mechanism to detect situations where multiple coordinator
     * checkpoints would be attempted overlapping, which is currently not supported (the gateway
     * doesn't keep a list of events blocked per checkpoint). It also helps to identify situations
     * where the checkpoint was aborted even before the gateway was closed (by finding out that the
     * {@code currentCheckpointId} was already reset to {@code NO_CHECKPOINT}.
     */
    void markForCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        if (currentCheckpointId != NO_CHECKPOINT && currentCheckpointId != checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot mark for checkpoint %d, already marked for checkpoint %d",
                            checkpointId, currentCheckpointId));
        }

        if (checkpointId > lastCheckpointId) {
            currentCheckpointId = checkpointId;
            lastCheckpointId = checkpointId;
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Regressing checkpoint IDs. Previous checkpointId = %d, new checkpointId = %d",
                            lastCheckpointId, checkpointId));
        }
    }

    /**
     * Closes the gateway. All events sent through this gateway are blocked until the gateway is
     * re-opened. If the gateway is already closed, this does nothing.
     *
     * @return True if the gateway is closed, false if the checkpointId is incorrect.
     */
    boolean tryCloseGateway(long checkpointId) {
        checkRunsInMainThread();

        if (checkpointId == currentCheckpointId) {
            isClosed = true;
            return true;
        }

        return false;
    }

    void openGatewayAndUnmarkCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        // Gateways should always be marked and closed for a specific checkpoint before it can be
        // reopened for that checkpoint. If a gateway is to be opened for an unforeseen checkpoint,
        // exceptions should be thrown.
        if (lastCheckpointId < checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "Gateway closed for different checkpoint: closed for = %d, expected = %d",
                            currentCheckpointId, checkpointId));
        }

        // The message to open gateway with a specific checkpoint id might arrive after the
        // checkpoint has been aborted, or even after a new checkpoint has started. In these cases
        // this message should be ignored.
        if (currentCheckpointId == NO_CHECKPOINT || checkpointId < lastCheckpointId) {
            return;
        }

        openGatewayAndUnmarkCheckpoint();
    }

    /** Opens the gateway, releasing all buffered events. */
    void openGatewayAndUnmarkCheckpoint() {
        checkRunsInMainThread();

        currentCheckpointId = NO_CHECKPOINT;
        if (!isClosed) {
            return;
        }

        for (BlockedEvent blockedEvent : blockedEvents) {
            callSendAction(blockedEvent.sendAction, blockedEvent.future);
        }
        blockedEvents.clear();

        isClosed = false;
    }

    private void checkRunsInMainThread() {
        mainThreadExecutor.assertRunningInMainThread();
    }

    private static final class BlockedEvent {

        private final Callable<CompletableFuture<Acknowledge>> sendAction;
        private final CompletableFuture<Acknowledge> future;

        BlockedEvent(
                Callable<CompletableFuture<Acknowledge>> sendAction,
                CompletableFuture<Acknowledge> future) {
            this.sendAction = sendAction;
            this.future = future;
        }
    }
}
