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
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;

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

    private final AsyncBatchFunction<IN, OUT> asyncBatchFunction;
    private final int maxBatchSize;

    public AsyncBatchWaitOperatorFactory(
            AsyncBatchFunction<IN, OUT> asyncBatchFunction, int maxBatchSize) {
        this.asyncBatchFunction = asyncBatchFunction;
        this.maxBatchSize = maxBatchSize;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        AsyncBatchWaitOperator<IN, OUT> operator =
                new AsyncBatchWaitOperator<>(
                        parameters, asyncBatchFunction, maxBatchSize, getMailboxExecutor());
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AsyncBatchWaitOperator.class;
    }
}
