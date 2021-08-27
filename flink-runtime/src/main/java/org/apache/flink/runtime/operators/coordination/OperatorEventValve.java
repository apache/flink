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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * The event value is the connection through which operator events are sent, from coordinator to
 * operator. It can temporarily block events from going through, buffering them, and releasing them
 * later. It is used for "alignment" of operator event streams with checkpoint barrier injection,
 * similar to how the input channels are aligned during a common checkpoint.
 *
 * <p>This class is NOT thread safe, but assumed to be used in a single threaded context. To guard
 * that, one can register a "main thread executor" (as used by the mailbox components like RPC
 * components) via {@link #setMainThreadExecutorForValidation(ComponentMainThreadExecutor)}.
 */
final class OperatorEventValve implements EventSender {

    private static final long NO_CHECKPOINT = Long.MIN_VALUE;

    private final List<BlockedEvent> blockedEvents = new ArrayList<>();

    private long currentCheckpointId;

    private long lastCheckpointId;

    private boolean shut;

    @Nullable private ComponentMainThreadExecutor mainThreadExecutor;

    /** Constructs a new OperatorEventValve. */
    OperatorEventValve() {
        this.currentCheckpointId = NO_CHECKPOINT;
        this.lastCheckpointId = Long.MIN_VALUE;
    }

    public void setMainThreadExecutorForValidation(ComponentMainThreadExecutor mainThreadExecutor) {
        this.mainThreadExecutor = mainThreadExecutor;
    }

    // ------------------------------------------------------------------------

    public boolean isShut() {
        checkRunsInMainThread();

        return shut;
    }

    /**
     * Send the event directly, if the valve is open, and returns the original sending result
     * future.
     *
     * <p>If the valve is closed this buffers the event and returns an incomplete future. The future
     * is completed with the original result once the valve is opened again.
     *
     * <p>This method makes no assumptions and gives no guarantees from which thread the result
     * future gets completed.
     */
    @Override
    public void sendEvent(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        checkRunsInMainThread();

        if (shut) {
            blockedEvents.add(new BlockedEvent(sendAction, result));
        } else {
            callSendAction(sendAction, result);
        }
    }

    /**
     * Marks the valve for the next checkpoint. This remembers the checkpoint ID and will only allow
     * shutting the value for this specific checkpoint.
     *
     * <p>This is the valve's mechanism to detect situations where multiple coordinator checkpoints
     * would be attempted overlapping, which is currently not supported (the valve doesn't keep a
     * list of events blocked per checkpoint). It also helps to identify situations where the
     * checkpoint was aborted even before the valve was shut (by finding out that the {@code
     * currentCheckpointId} was already reset to {@code NO_CHECKPOINT}.
     */
    public void markForCheckpoint(long checkpointId) {
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
     * Shuts the value. All events sent through this valve are blocked until the valve is re-opened.
     * If the valve is already shut, this does nothing.
     */
    public boolean tryShutValve(long checkpointId) {
        checkRunsInMainThread();

        if (checkpointId == currentCheckpointId) {
            shut = true;
            return true;
        }
        return false;
    }

    public void openValveAndUnmarkCheckpoint(long expectedCheckpointId) {
        checkRunsInMainThread();

        if (expectedCheckpointId != currentCheckpointId) {
            throw new IllegalStateException(
                    String.format(
                            "Valve closed for different checkpoint: closed for = %d, expected = %d",
                            currentCheckpointId, expectedCheckpointId));
        }
        openValveAndUnmarkCheckpoint();
    }

    /** Opens the value, releasing all buffered events. */
    public void openValveAndUnmarkCheckpoint() {
        checkRunsInMainThread();

        currentCheckpointId = NO_CHECKPOINT;
        if (!shut) {
            return;
        }

        for (BlockedEvent blockedEvent : blockedEvents) {
            callSendAction(blockedEvent.sendAction, blockedEvent.future);
        }
        blockedEvents.clear();
        shut = false;
    }

    private void checkRunsInMainThread() {
        if (mainThreadExecutor != null) {
            mainThreadExecutor.assertRunningInMainThread();
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

    // ------------------------------------------------------------------------

    private static final class BlockedEvent {

        final Callable<CompletableFuture<Acknowledge>> sendAction;
        final CompletableFuture<Acknowledge> future;

        BlockedEvent(
                Callable<CompletableFuture<Acknowledge>> sendAction,
                CompletableFuture<Acknowledge> future) {
            this.sendAction = sendAction;
            this.future = future;
        }
    }
}
