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

package org.apache.flink.runtime.taskprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

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
@Internal
public class AsyncExecutionController<R, K> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecutionController.class);

    public static final int DEFAULT_MAX_IN_FLIGHT_RECORD_NUM = 6000;

    /** The max allow number of in-flight records. */
    private final int maxInFlightRecordNum;

    /** The key accounting unit which is used to detect the key conflict. */
    private final KeyAccountingUnit<R, K> keyAccountingUnit;

    /** The mailbox executor, borrowed from {@code StreamTask}. */
    private final MailboxExecutor mailboxExecutor;

    /** The state executor where the {@link ProcessingRequest} is actually executed. */
    private final StateExecutor stateExecutor;

    public AsyncExecutionController(MailboxExecutor mailboxExecutor, StateExecutor stateExecutor) {
        this(mailboxExecutor, stateExecutor, DEFAULT_MAX_IN_FLIGHT_RECORD_NUM);
    }

    public AsyncExecutionController(
            MailboxExecutor mailboxExecutor, StateExecutor stateExecutor, int maxInFlightRecords) {
        this.mailboxExecutor = mailboxExecutor;
        this.stateExecutor = stateExecutor;
        this.maxInFlightRecordNum = maxInFlightRecords;
        this.keyAccountingUnit = new KeyAccountingUnit<>();
        LOG.info("Create AsyncExecutionController: maxInFlightRecordsNum {}", maxInFlightRecords);
    }

    public <OUT> void handleProcessingRequest(
            ProcessingRequest<OUT> request, RecordContext<K, R> recordContext) {
        // TODO(implement): preserve key order
        stateExecutor.executeBatchRequests(Collections.singleton(request));
    }
}
