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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.core.state.InternalStateFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
 * @param <R> the type of the record
 * @param <K> the type of the key
 */
public class AsyncExecutionController<R, K> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecutionController.class);

    public static final int DEFAULT_MAX_IN_FLIGHT_RECORD_NUM = 6000;

    /** The max allowed number of in-flight records. */
    private final int maxInFlightRecordNum;

    /** The key accounting unit which is used to detect the key conflict. */
    final KeyAccountingUnit<R, K> keyAccountingUnit;

    /**
     * A factory to build {@link org.apache.flink.core.state.InternalStateFuture}, this will auto
     * wire the created future with mailbox executor. Also conducting the context switch.
     */
    private final StateFutureFactory<R, K> stateFutureFactory;

    /** The state executor where the {@link StateRequest} is actually executed. */
    final StateExecutor stateExecutor;

    /** The corresponding context that currently runs in task thread. */
    RecordContext<R, K> currentContext;

    public AsyncExecutionController(MailboxExecutor mailboxExecutor, StateExecutor stateExecutor) {
        this(mailboxExecutor, stateExecutor, DEFAULT_MAX_IN_FLIGHT_RECORD_NUM);
    }

    public AsyncExecutionController(
            MailboxExecutor mailboxExecutor, StateExecutor stateExecutor, int maxInFlightRecords) {
        this.keyAccountingUnit = new KeyAccountingUnit<>(maxInFlightRecords);
        this.stateFutureFactory = new StateFutureFactory<>(this, mailboxExecutor);
        this.stateExecutor = stateExecutor;
        this.maxInFlightRecordNum = maxInFlightRecords;
        LOG.info("Create AsyncExecutionController: maxInFlightRecordsNum {}", maxInFlightRecords);
    }

    /**
     * Build a new context based on record and key. Also wired with internal {@link
     * KeyAccountingUnit}.
     *
     * @param record the given record.
     * @param key the given key.
     * @return the built record context.
     */
    public RecordContext<R, K> buildContext(R record, K key) {
        return new RecordContext<>(record, key, this::disposeContext);
    }

    /**
     * Each time before a code segment (callback) is about to run in mailbox (task thread), this
     * method should be called to switch a context in AEC.
     *
     * @param switchingContext the context to switch.
     */
    public void setCurrentContext(RecordContext<R, K> switchingContext) {
        currentContext = switchingContext;
    }

    /**
     * Dispose a context.
     *
     * @param toDispose the context to dispose.
     */
    public void disposeContext(RecordContext<R, K> toDispose) {
        keyAccountingUnit.release(toDispose.getRecord(), toDispose.getKey());
    }

    /**
     * Try to occupy a key by a given context.
     *
     * @param recordContext the given context.
     * @return true if occupy succeed or the key has already occupied by this context.
     */
    boolean tryOccupyKey(RecordContext<R, K> recordContext) {
        boolean occupied = recordContext.isKeyOccupied();
        if (!occupied
                && keyAccountingUnit.occupy(recordContext.getRecord(), recordContext.getKey())) {
            recordContext.setKeyOccupied();
            occupied = true;
        }
        return occupied;
    }

    /**
     * Submit a {@link StateRequest} to this AEC and trigger if needed.
     *
     * @param state the state to request. Could be {@code null} if the type is {@link
     *     StateRequestType#SYNC_POINT}.
     * @param type the type of this request.
     * @param payload the payload input for this request.
     * @return the state future.
     */
    public <IN, OUT> InternalStateFuture<OUT> handleRequest(
            @Nullable State state, StateRequestType type, @Nullable IN payload) {
        // Step 1: build state future & assign context.
        InternalStateFuture<OUT> stateFuture = stateFutureFactory.create(currentContext);
        StateRequest<K, IN, OUT> request =
                new StateRequest<>(state, type, payload, stateFuture, currentContext);
        // Step 2: try to occupy the key and place it into right buffer.
        if (tryOccupyKey(currentContext)) {
            insertActiveBuffer(request);
        } else {
            insertBlockingBuffer(request);
        }
        // Step 3: trigger the (active) buffer if needed.
        triggerIfNeeded(false);
        return stateFuture;
    }

    <IN, OUT> void insertActiveBuffer(StateRequest<K, IN, OUT> request) {
        // TODO: implement the active buffer.
    }

    <IN, OUT> void insertBlockingBuffer(StateRequest<K, IN, OUT> request) {
        // TODO: implement the blocking buffer.
    }

    /**
     * Trigger a batch of requests.
     *
     * @param force whether to trigger requests in force.
     */
    void triggerIfNeeded(boolean force) {
        // TODO: implement the trigger logic.
    }
}
