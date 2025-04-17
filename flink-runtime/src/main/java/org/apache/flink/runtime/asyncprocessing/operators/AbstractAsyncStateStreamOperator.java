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

package org.apache.flink.runtime.asyncprocessing.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.StateExecutionController;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessing;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessingOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This operator is an abstract class that give the {@link AbstractStreamOperator} the ability to
 * perform {@link AsyncKeyOrderedProcessing}. The aim is to make any subclass of {@link
 * AbstractStreamOperator} could manipulate async state with only a change of base class.
 */
@Internal
@SuppressWarnings("rawtypes")
public abstract class AbstractAsyncStateStreamOperator<OUT>
        extends AbstractAsyncKeyOrderedStreamOperator<OUT>
        implements AsyncKeyOrderedProcessingOperator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractAsyncStateStreamOperator.class);

    @Override
    protected KeySelector getKeySelectorForAsyncKeyedContext(int index) {
        switch (index) {
            case 1:
                return stateKeySelector1;
            case 2:
                return stateKeySelector2;
            default:
                throw new ArrayIndexOutOfBoundsException(
                        "Try to get key selector for index " + index);
        }
    }

    @Override
    protected AsyncExecutionController createAsyncExecutionController() {
        final StreamTask<?, ?> containingTask = checkNotNull(getContainingTask());
        final MailboxExecutor mailboxExecutor =
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(getOperatorConfig().getChainIndex());
        final int maxParallelism = environment.getTaskInfo().getMaxNumberOfParallelSubtasks();
        final int inFlightRecordsLimit =
                environment.getExecutionConfig().getAsyncStateTotalBufferSize();
        final int asyncBufferSize =
                environment.getExecutionConfig().getAsyncStateActiveBufferSize();
        final long asyncBufferTimeout =
                environment.getExecutionConfig().getAsyncStateActiveBufferTimeout();

        if (this.isAsyncKeyOrderedProcessingEnabled()) {
            AsyncKeyedStateBackend asyncKeyedStateBackend =
                    stateHandler.getAsyncKeyedStateBackend();
            if (asyncKeyedStateBackend != null) {
                StateExecutionController asyncExecutionController =
                        new StateExecutionController(
                                mailboxExecutor,
                                this::handleAsyncException,
                                asyncKeyedStateBackend.createStateExecutor(),
                                declarationManager,
                                EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                                maxParallelism,
                                asyncBufferSize,
                                asyncBufferTimeout,
                                inFlightRecordsLimit,
                                asyncKeyedStateBackend,
                                getMetricGroup().addGroup("asyncStateProcessing"));
                asyncKeyedStateBackend.setup(asyncExecutionController);
                if (asyncKeyedStateBackend instanceof AsyncKeyedStateBackendAdaptor) {
                    LOG.warn(
                            "A normal KeyedStateBackend({}) is used when enabling the async state "
                                    + "processing. Parallel asynchronous processing does not work. "
                                    + "All state access will be processed synchronously.",
                            stateHandler.getKeyedStateBackend());
                }
                return asyncExecutionController;
            } else if (stateHandler.getKeyedStateBackend() != null) {
                throw new UnsupportedOperationException(
                        "Current State Backend doesn't support async access, AsyncExecutionController could not work");
            }
        }
        return null;
    }
}
