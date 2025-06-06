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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.core.state.StateFutureImpl.AsyncFrameworkExceptionHandler;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.EpochManager.ParallelMode;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Async Execution Controller (AEC) receives processing requests from operators, and put them
 * into execution according to some strategies.
 *
 * <p>It is responsible for:
 * <li>Preserving the sequence of elements bearing the same key by delaying subsequent requests
 *     until the processing of preceding ones is finalized.
 * <li>Tracking the in-flight data(records) and blocking the input if too much data in flight
 *     (back-pressure). It invokes {@link MailboxExecutor#yield()} to pause current operations,
 *     allowing for the execution of callbacks (mails in Mailbox).
 *
 * @param <K> the type of the key
 */
public class AsyncExecutionController<K> implements StateRequestHandler, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecutionController.class);

    private static final long DEFAULT_BUFFER_TIMEOUT_CHECK_INTERVAL = 100;

    /**
     * The batch size. When the number of state requests in the active buffer exceeds the batch
     * size, a batched state execution would be triggered.
     */
    private final int batchSize;

    /** The runner for callbacks. Basically a wrapper of mailbox executor. */
    private final CallbackRunnerWrapper callbackRunner;

    /**
     * The timeout of {@link StateRequestBuffer#activeQueue} triggering in milliseconds. If the
     * activeQueue has not reached the {@link #batchSize} within 'buffer-timeout' milliseconds, a
     * trigger will perform actively.
     */
    private final long bufferTimeout;

    /**
     * There might be huge overhead when inserting a timer for each buffer. A periodic check is a
     * good trade-off to save much GC and CPU for this. This var defines the interval for periodic
     * check of timeout. As a result, the real trigger time of timeout buffer might be [timeout,
     * timeout+interval]. We don't make it configurable for now.
     */
    private final long bufferTimeoutCheckInterval = DEFAULT_BUFFER_TIMEOUT_CHECK_INTERVAL;

    /** The max allowed number of in-flight records. */
    private final int maxInFlightRecordNum;

    /**
     * The mailbox executor borrowed from {@code StreamTask}. Keeping the reference of
     * mailboxExecutor here is to restrict the number of in-flight records, when the number of
     * in-flight records > {@link #maxInFlightRecordNum}, the newly entering records would be
     * blocked.
     */
    private final MailboxExecutor mailboxExecutor;

    /** Exception handler to handle the exception thrown by asynchronous framework. */
    private final AsyncFrameworkExceptionHandler exceptionHandler;

    /** The key accounting unit which is used to detect the key conflict. */
    final KeyAccountingUnit<K> keyAccountingUnit;

    /**
     * A factory to build {@link org.apache.flink.core.state.InternalStateFuture}, this will auto
     * wire the created future with mailbox executor. Also conducting the context switch.
     */
    private final StateFutureFactory<K> stateFutureFactory;

    /** The state executor where the {@link StateRequest} is actually executed. */
    private final StateExecutor stateExecutor;

    /** A manager that allows for declaring processing and variables. */
    private final DeclarationManager declarationManager;

    /** The corresponding context that currently runs in task thread. */
    RecordContext<K> currentContext;

    /** The buffer to store the state requests to execute in batch. */
    StateRequestBuffer<K> stateRequestsBuffer;

    /**
     * The number of in-flight records. Including the records in active buffer and blocking buffer.
     */
    final AtomicInteger inFlightRecordNum;

    /** Max parallelism of the job. */
    private final int maxParallelism;

    /** The reference of epoch manager. */
    final EpochManager epochManager;

    /** The listener of context switch. */
    final SwitchContextListener<K> switchContextListener;

    /**
     * The parallel mode of epoch execution. Keep this field internal for now, until we could see
     * the concrete need for {@link ParallelMode#PARALLEL_BETWEEN_EPOCH} from average users.
     */
    final ParallelMode epochParallelMode = ParallelMode.SERIAL_BETWEEN_EPOCH;

    /** A guard for waiting new mail. */
    private final Object notifyLock = new Object();

    /** Flag indicating if this AEC is under waiting status. */
    private volatile boolean waitingMail = false;

    /** The recursive depth of the drain process. */
    private int drainDepth = 0;

    public AsyncExecutionController(
            MailboxExecutor mailboxExecutor,
            AsyncFrameworkExceptionHandler exceptionHandler,
            StateExecutor stateExecutor,
            DeclarationManager declarationManager,
            int maxParallelism,
            int batchSize,
            long bufferTimeout,
            int maxInFlightRecords,
            @Nullable SwitchContextListener<K> switchContextListener,
            @Nullable MetricGroup metricGroup) {
        this.keyAccountingUnit = new KeyAccountingUnit<>(maxInFlightRecords);
        this.mailboxExecutor = mailboxExecutor;
        this.exceptionHandler = exceptionHandler;
        this.callbackRunner = new CallbackRunnerWrapper(mailboxExecutor, this::notifyNewMail);
        this.stateFutureFactory = new StateFutureFactory<>(this, callbackRunner, exceptionHandler);

        this.stateExecutor = stateExecutor;
        this.declarationManager = declarationManager;
        this.batchSize = batchSize;
        this.bufferTimeout = bufferTimeout;
        this.maxInFlightRecordNum = maxInFlightRecords;
        this.inFlightRecordNum = new AtomicInteger(0);
        this.maxParallelism = maxParallelism;
        this.stateRequestsBuffer =
                new StateRequestBuffer<>(
                        bufferTimeout,
                        bufferTimeoutCheckInterval,
                        (scheduledSeq) ->
                                mailboxExecutor.execute(
                                        () -> {
                                            if (stateRequestsBuffer.checkCurrentSeq(scheduledSeq)) {
                                                triggerIfNeeded(true);
                                            }
                                        },
                                        "AEC-buffer-timeout"));

        this.epochManager = new EpochManager(this);
        this.switchContextListener = switchContextListener;
        if (metricGroup != null) {
            metricGroup.gauge("numInFlightRecords", this::getInFlightRecordNum);
            metricGroup.gauge("activeBufferSize", () -> stateRequestsBuffer.activeQueueSize());
            metricGroup.gauge("blockingBufferSize", () -> stateRequestsBuffer.blockingQueueSize());
            metricGroup.gauge("numBlockingKeys", () -> stateRequestsBuffer.blockingKeyNum());
        }
        LOG.info(
                "Create AsyncExecutionController: batchSize {}, bufferTimeout {}, maxInFlightRecordNum {}, epochParallelMode {}",
                this.batchSize,
                this.bufferTimeout,
                this.maxInFlightRecordNum,
                this.epochParallelMode);
    }

    /**
     * Build a new context based on record and key. Also wired with internal {@link
     * KeyAccountingUnit}.
     *
     * @param record the given record.
     * @param key the given key.
     * @return the built record context.
     */
    public RecordContext<K> buildContext(Object record, K key) {
        return buildContext(record, key, false);
    }

    /**
     * Build a new context based on record and key. Also wired with internal {@link
     * KeyAccountingUnit}.
     *
     * @param record the given record.
     * @param key the given key.
     * @param inherit whether to inherit epoch and variables from the current context. Or otherwise
     *     create new ones.
     * @return the built record context.
     */
    public RecordContext<K> buildContext(Object record, K key, boolean inherit) {
        if (inherit && currentContext != null) {
            return new RecordContext<>(
                    record == null ? RecordContext.EMPTY_RECORD : record,
                    key,
                    this::disposeContext,
                    KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism),
                    epochManager.onEpoch(currentContext.getEpoch()),
                    currentContext.getVariablesReference(),
                    // When inheriting, we increase the priority by 1 to ensure that the record is
                    // processed right after the current record if possible.
                    currentContext.getPriority() + 1);
        } else {
            return new RecordContext<>(
                    record == null ? RecordContext.EMPTY_RECORD : record,
                    key,
                    this::disposeContext,
                    KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism),
                    epochManager.onRecord(),
                    declarationManager.variableCount());
        }
    }

    /**
     * Each time before a code segment (callback) is about to run in mailbox (task thread), this
     * method should be called to switch a context in AEC.
     *
     * @param switchingContext the context to switch.
     */
    public void setCurrentContext(RecordContext<K> switchingContext) {
        if (currentContext != switchingContext) {
            currentContext = switchingContext;
            declarationManager.setCurrentContext(switchingContext);
            if (switchContextListener != null) {
                switchContextListener.switchContext(switchingContext);
            }
        }
    }

    public RecordContext<K> getCurrentContext() {
        return currentContext;
    }

    /**
     * Dispose a context.
     *
     * @param toDispose the context to dispose.
     */
    void disposeContext(RecordContext<K> toDispose) {
        epochManager.completeOneRecord(toDispose.getEpoch());
        keyAccountingUnit.release(toDispose.getRecord(), toDispose.getKey());
        inFlightRecordNum.decrementAndGet();
        StateRequest<K, ?, ?, ?> nextRequest =
                stateRequestsBuffer.unblockOneByKey(toDispose.getKey());
        if (nextRequest != null) {
            Preconditions.checkState(tryOccupyKey(nextRequest.getRecordContext()));
            insertActiveBuffer(nextRequest);
        }
    }

    /**
     * Try to occupy a key by a given context.
     *
     * @param recordContext the given context.
     * @return true if occupy succeed or the key has already occupied by this context.
     */
    boolean tryOccupyKey(RecordContext<K> recordContext) {
        boolean occupied = recordContext.isKeyOccupied();
        if (!occupied
                && keyAccountingUnit.occupy(recordContext.getRecord(), recordContext.getKey())) {
            recordContext.setKeyOccupied();
            occupied = true;
        }
        return occupied;
    }

    /**
     * Submit a {@link StateRequest} to this AsyncExecutionController and trigger it if needed.
     *
     * @param state the state to request. Could be {@code null} if the type is {@link
     *     StateRequestType#SYNC_POINT}.
     * @param type the type of this request.
     * @param payload the payload input for this request.
     * @return the state future.
     */
    @Override
    public <IN, OUT> InternalStateFuture<OUT> handleRequest(
            @Nullable State state, StateRequestType type, @Nullable IN payload) {
        return handleRequest(state, type, false, payload, false);
    }

    /**
     * Submit a {@link StateRequest} to this AsyncExecutionController and trigger it if needed.
     *
     * @param state the state to request. Could be {@code null} if the type is {@link
     *     StateRequestType#SYNC_POINT}.
     * @param type the type of this request.
     * @param sync whether to trigger the request synchronously once it's ready.
     * @param payload the payload input for this request.
     * @param allowOverdraft whether to allow overdraft.
     * @return the state future.
     */
    public <IN, OUT> InternalStateFuture<OUT> handleRequest(
            @Nullable State state,
            StateRequestType type,
            boolean sync,
            @Nullable IN payload,
            boolean allowOverdraft) {
        // Step 1: build state future & assign context.
        InternalStateFuture<OUT> stateFuture = stateFutureFactory.create(currentContext);
        StateRequest<K, ?, IN, OUT> request =
                new StateRequest<>(
                        state,
                        type,
                        sync || type == StateRequestType.SYNC_POINT,
                        payload,
                        stateFuture,
                        currentContext);

        // Step 2: try to seize the capacity, if the current in-flight records exceeds the limit,
        // block the current state request from entering until some buffered requests are processed.
        seizeCapacity(allowOverdraft);

        // Step 3: try to occupy the key and place it into right buffer.
        if (tryOccupyKey(currentContext)) {
            insertActiveBuffer(request);
        } else {
            insertBlockingBuffer(request);
        }
        // Step 4: trigger the (active) buffer if needed.
        triggerIfNeeded(false);
        return stateFuture;
    }

    @Override
    public <IN, OUT> OUT handleRequestSync(
            State state, StateRequestType type, @Nullable IN payload) {
        InternalStateFuture<OUT> stateFuture = handleRequest(state, type, true, payload, false);
        if (!stateFuture.isDone()) {
            // Trigger since we are waiting the result.
            triggerIfNeeded(true);
            try {
                while (!stateFuture.isDone()) {
                    if (!mailboxExecutor.tryYield()) {
                        // We force trigger the buffer if the executor is not fully loaded.
                        if (!stateExecutor.fullyLoaded()) {
                            triggerIfNeeded(true);
                        }
                        waitForNewMails();
                    }
                }
            } catch (InterruptedException ignored) {
                // ignore the interrupted exception to avoid throwing fatal error when the task
                // cancel
                // or exit.
            }
        }
        return stateFuture.get();
    }

    @Override
    public <N> void setCurrentNamespaceForState(
            @Nonnull InternalPartitionedState<N> state, N namespace) {
        currentContext.setNamespace(state, namespace);
    }

    <IN, OUT> void insertActiveBuffer(StateRequest<K, ?, IN, OUT> request) {
        if (request.isSync()) {
            if (request.getRequestType() == StateRequestType.SYNC_POINT) {
                request.getFuture().complete(null);
            } else {
                stateExecutor.executeRequestSync(request);
            }
        } else {
            stateRequestsBuffer.enqueueToActive(request);
        }
    }

    <IN, OUT> void insertBlockingBuffer(StateRequest<K, ?, IN, OUT> request) {
        stateRequestsBuffer.enqueueToBlocking(request);
    }

    /**
     * Trigger a batch of requests.
     *
     * @param force whether to trigger requests in force.
     */
    public boolean triggerIfNeeded(boolean force) {
        if (!force && stateRequestsBuffer.activeQueueSize() < batchSize) {
            return false;
        }

        Optional<StateRequestContainer> toRun =
                stateRequestsBuffer.popActive(
                        batchSize, () -> stateExecutor.createStateRequestContainer());
        if (!toRun.isPresent() || toRun.get().isEmpty()) {
            return false;
        }
        stateExecutor.executeBatchRequests(toRun.get());
        stateRequestsBuffer.advanceSeq();
        return true;
    }

    /**
     * Seize capacity from the in-flight request limit. Will drain if reach the limit.
     *
     * @param allowOverdraft whether to allow overdraft. If true, it won't drain the in-flight
     *     requests even though it reaches the limit.
     */
    private void seizeCapacity(boolean allowOverdraft) {
        // 1. Check if the record is already in buffer. If yes, this indicates that it is a state
        // request resulting from a callback statement, otherwise, it signifies the initial state
        // request for a newly entered record.
        if (currentContext.isKeyOccupied()) {
            return;
        }
        // 2. If the state request is for a newly entered record, the in-flight record number should
        // be less than the max in-flight record number.
        // Note: the currentContext may be updated by {@code StateFutureFactory#build}.

        // We allow a derived request by another request (by initializing a process directly via
        // #asyncProcessWithKey, or timer triggering right after a record processing), meaning
        // that we are in middle of another processing and creating a new one here. If we block
        // here, there might be a deadlock (current processing waiting here to drain the current
        // processing, this is a rare case when all the records share the same key).
        // This probably cause the number of records actually run to be greater than the limit.
        // But overall it is under-control since there should not be many derived requests
        // within each request.
        // We perform a drain to keep the buffer limit. But when allowing overdraft, we won't wait
        // here.
        drainInflightRecords(maxInFlightRecordNum, !allowOverdraft);
        inFlightRecordNum.incrementAndGet();
    }

    /**
     * A helper to request a {@link StateRequestType#SYNC_POINT} and run a callback if it finishes
     * (once the record is not blocked).
     *
     * @param callback the callback to run if it finishes (once the record is not blocked).
     * @param allowOverdraft whether to overdraft the in-flight buffer.
     */
    public StateFuture<Void> syncPointRequestWithCallback(
            ThrowingRunnable<Exception> callback, boolean allowOverdraft) {
        return handleRequest(null, StateRequestType.SYNC_POINT, true, null, allowOverdraft)
                .thenAccept(v -> callback.run());
    }

    /**
     * A helper function to drain in-flight records util {@link #inFlightRecordNum} within the limit
     * of given {@code targetNum}.
     *
     * @param targetNum the target {@link #inFlightRecordNum} to achieve.
     */
    public void drainInflightRecords(int targetNum) {
        drainInflightRecords(targetNum, true);
    }

    /**
     * A helper function to drain in-flight records util {@link #inFlightRecordNum} within the limit
     * of given {@code targetNum}.
     *
     * @param targetNum the target {@link #inFlightRecordNum} to achieve.
     * @param forceToWait whether to force to wait until the target number is reached. If false,
     *     only drain in best efforts and return when no progress is made.
     */
    private void drainInflightRecords(int targetNum, boolean forceToWait) {
        if (!forceToWait && drainDepth > 5) {
            // We don't allow recursive call of drain if we are not forced to wait here.
            // This is to avoid stack overflow, since the yield will pick up another processing,
            // which may cause another drain.
            return;
        }
        // Store the current context, which might be switched below.
        RecordContext<K> storedContext = currentContext;
        drainDepth++;
        try {
            boolean shouldWait = true;
            while (shouldWait && inFlightRecordNum.get() > targetNum) {
                if (!mailboxExecutor.tryYield()) {
                    boolean triggered = false;
                    // We force trigger the buffer if targetNum == 0 (for draining) or the state
                    // executor is not fully loaded.
                    if (targetNum == 0 || !stateExecutor.fullyLoaded()) {
                        triggered = triggerIfNeeded(true);
                    }
                    if (!forceToWait
                            && !triggered
                            && !stateExecutor.fullyLoaded()
                            && !callbackRunner.isHasMail()) {
                        // Decision of waiting is based on whether we are making progress of state
                        // accessing (or if there is a deadlock). Based on the following factors:
                        // 1. We failed triggered some state requests. AND
                        // 2. The state executor is not fully loaded, meaning that state is not
                        // being accessed. AND
                        // 3. There is no new mail, meaning that the mailbox has no callbacks ready.
                        //
                        // We cannot make progress anywhere, then there probably is a deadlock. We'd
                        // better give up waiting.
                        shouldWait = false;
                        // What if we force to wait here but cannot make any progress?
                        // There must be a bug. TODO: Print necessary debug info in this case.
                    } else {
                        waitForNewMails();
                    }
                }
            }
        } catch (InterruptedException ignored) {
            // ignore the interrupted exception to avoid throwing fatal error when the task cancel
            // or exit.
        } finally {
            drainDepth--;
            // Restore the previously stored context.
            setCurrentContext(storedContext);
        }
    }

    /** Wait for new mails if there is no more mail. */
    private void waitForNewMails() throws InterruptedException {
        if (!callbackRunner.isHasMail()) {
            synchronized (notifyLock) {
                if (!callbackRunner.isHasMail()) {
                    waitingMail = true;
                    notifyLock.wait(1);
                    waitingMail = false;
                }
            }
        }
    }

    /** Notify this AEC there is a mail, wake up from waiting. */
    private void notifyNewMail() {
        if (waitingMail) {
            synchronized (notifyLock) {
                if (waitingMail) {
                    notifyLock.notify();
                }
            }
        }
    }

    public void processNonRecord(
            @Nullable ThrowingRunnable<? extends Exception> triggerAction,
            @Nullable ThrowingRunnable<? extends Exception> finalAction) {
        epochManager.onNonRecord(
                triggerAction == null
                        ? null
                        : () -> {
                            try {
                                // We clear the current context since this is a non-record context.
                                RecordContext<K> previousContext = currentContext;
                                setCurrentContext(null);
                                triggerAction.run();
                                setCurrentContext(previousContext);
                            } catch (Exception e) {
                                exceptionHandler.handleException(
                                        "Failed to process non-record.", e);
                            }
                        },
                finalAction == null
                        ? null
                        : () -> {
                            try {
                                RecordContext<K> previousContext = currentContext;
                                setCurrentContext(null);
                                finalAction.run();
                                setCurrentContext(previousContext);
                            } catch (Exception e) {
                                exceptionHandler.handleException(
                                        "Failed to process non-record.", e);
                            }
                        },
                epochParallelMode);
    }

    @VisibleForTesting
    public StateExecutor getStateExecutor() {
        return stateExecutor;
    }

    @VisibleForTesting
    public int getInFlightRecordNum() {
        return inFlightRecordNum.get();
    }

    @VisibleForTesting
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public void close() throws IOException {
        stateRequestsBuffer.close();
    }

    /** A listener listens the key context switch. */
    public interface SwitchContextListener<K> {
        void switchContext(@Nullable RecordContext<K> context);
    }
}
