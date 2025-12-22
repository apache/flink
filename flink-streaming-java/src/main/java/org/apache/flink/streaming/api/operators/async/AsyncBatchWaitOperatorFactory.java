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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.AsyncBatchFunction;
import org.apache.flink.streaming.api.functions.async.AsyncBatchRetryStrategy;
import org.apache.flink.streaming.api.functions.async.AsyncBatchTimeoutPolicy;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.streaming.util.retryable.AsyncBatchRetryStrategies;

/**
 * The factory of {@link AsyncBatchWaitOperator}.
 *
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@Internal
public class AsyncBatchWaitOperatorFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>, YieldingOperatorFactory<OUT> {

    private static final long serialVersionUID = 1L;

    /** Constant indicating timeout is disabled. */
    private static final long NO_TIMEOUT = 0L;

    private final AsyncBatchFunction<IN, OUT> asyncBatchFunction;
    private final int maxBatchSize;
    private final long batchTimeoutMs;
    private final AsyncBatchRetryStrategy<OUT> retryStrategy;
    private final AsyncBatchTimeoutPolicy timeoutPolicy;

    /**
     * Creates a factory with size-based batching only (no timeout, no retry, no async timeout).
     *
     * @param asyncBatchFunction The async batch function
     * @param maxBatchSize Maximum batch size before triggering async invocation
     */
    public AsyncBatchWaitOperatorFactory(
            AsyncBatchFunction<IN, OUT> asyncBatchFunction, int maxBatchSize) {
        this(asyncBatchFunction, maxBatchSize, NO_TIMEOUT);
    }

    /**
     * Creates a factory with size-based and optional timeout-based batching (no retry, no async
     * timeout).
     *
     * @param asyncBatchFunction The async batch function
     * @param maxBatchSize Maximum batch size before triggering async invocation
     * @param batchTimeoutMs Batch timeout in milliseconds; <= 0 means disabled
     */
    @SuppressWarnings("unchecked")
    public AsyncBatchWaitOperatorFactory(
            AsyncBatchFunction<IN, OUT> asyncBatchFunction, int maxBatchSize, long batchTimeoutMs) {
        this(
                asyncBatchFunction,
                maxBatchSize,
                batchTimeoutMs,
                AsyncBatchRetryStrategies.noRetry(),
                AsyncBatchTimeoutPolicy.NO_TIMEOUT_POLICY);
    }

    /**
     * Creates a factory with full configuration including retry and timeout policies.
     *
     * @param asyncBatchFunction The async batch function
     * @param maxBatchSize Maximum batch size before triggering async invocation
     * @param batchTimeoutMs Batch timeout in milliseconds; <= 0 means disabled
     * @param retryStrategy Retry strategy for failed batch operations
     * @param timeoutPolicy Timeout policy for async batch operations
     */
    public AsyncBatchWaitOperatorFactory(
            AsyncBatchFunction<IN, OUT> asyncBatchFunction,
            int maxBatchSize,
            long batchTimeoutMs,
            AsyncBatchRetryStrategy<OUT> retryStrategy,
            AsyncBatchTimeoutPolicy timeoutPolicy) {
        this.asyncBatchFunction = asyncBatchFunction;
        this.maxBatchSize = maxBatchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.retryStrategy = retryStrategy;
        this.timeoutPolicy = timeoutPolicy;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        AsyncBatchWaitOperator<IN, OUT> operator =
                new AsyncBatchWaitOperator<>(
                        parameters,
                        asyncBatchFunction,
                        maxBatchSize,
                        batchTimeoutMs,
                        getMailboxExecutor(),
                        retryStrategy,
                        timeoutPolicy);
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AsyncBatchWaitOperator.class;
    }
}
