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
import org.apache.flink.core.asyncprocessing.AsyncFuture;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.SimpleAsyncExecutionController;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessing;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncKeyOrderedProcessingOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.function.CheckedSupplier;

import java.util.concurrent.ExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This operator is an abstract class that give the {@link AbstractStreamOperator} the ability to
 * perform {@link AsyncKeyOrderedProcessing}. Note that this operator is not under keyed context,
 * but providing key selectors for the async processing.
 */
@Internal
@SuppressWarnings("rawtypes")
public abstract class AbstractAsyncRunnableStreamOperator<OUT>
        extends AbstractAsyncKeyOrderedStreamOperator<OUT>
        implements AsyncKeyOrderedProcessingOperator {

    final KeySelector<?, ?> keySelector1;
    final KeySelector<?, ?> keySelector2;
    final ExecutorService asyncThreadPool;
    final int asyncBufferSize;
    final long asyncBufferTimeout;
    final int inFlightRecordsLimit;

    public AbstractAsyncRunnableStreamOperator(
            KeySelector<?, ?> keySelector1,
            KeySelector<?, ?> keySelector2,
            ExecutorService asyncThreadPool,
            int asyncBufferSize,
            long asyncBufferTimeout,
            int inFlightRecordsLimit) {
        this.keySelector1 = keySelector1;
        this.keySelector2 = keySelector2;
        this.asyncThreadPool = asyncThreadPool;
        this.asyncBufferSize = asyncBufferSize;
        this.asyncBufferTimeout = asyncBufferTimeout;
        this.inFlightRecordsLimit = inFlightRecordsLimit;
    }

    @Override
    protected KeySelector getKeySelectorForAsyncKeyedContext(int index) {
        switch (index) {
            case 1:
                return keySelector1;
            case 2:
                return keySelector2;
            default:
                throw new ArrayIndexOutOfBoundsException(
                        "Try to get key selector for index " + index);
        }
    }

    @Override
    protected AsyncExecutionController createAsyncExecutionController() {
        if (isAsyncKeyOrderedProcessingEnabled()) {
            final StreamTask<?, ?> containingTask = checkNotNull(getContainingTask());
            final MailboxExecutor mailboxExecutor =
                    containingTask
                            .getMailboxExecutorFactory()
                            .createExecutor(getOperatorConfig().getChainIndex());
            final int maxParallelism = environment.getTaskInfo().getMaxNumberOfParallelSubtasks();
            return new SimpleAsyncExecutionController(
                    mailboxExecutor,
                    this::handleAsyncException,
                    asyncThreadPool,
                    getDeclarationManager(),
                    getEpochParallelMode(),
                    maxParallelism,
                    asyncBufferSize,
                    asyncBufferTimeout,
                    inFlightRecordsLimit,
                    null,
                    getMetricGroup().addGroup("asyncProcessing"));
        }
        return null;
    }

    /**
     * Define the parallel mode of the epoch manager. The default is {@link
     * EpochManager.ParallelMode#SERIAL_BETWEEN_EPOCH}. Subclasses can override this method to
     * change the parallel mode.
     */
    protected EpochManager.ParallelMode getEpochParallelMode() {
        return EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH;
    }

    @SuppressWarnings("unchecked")
    protected <RET> AsyncFuture<RET> asyncProcess(CheckedSupplier<RET> runnable) {
        return ((SimpleAsyncExecutionController) asyncExecutionController)
                .handleRequest(runnable, false);
    }
}
