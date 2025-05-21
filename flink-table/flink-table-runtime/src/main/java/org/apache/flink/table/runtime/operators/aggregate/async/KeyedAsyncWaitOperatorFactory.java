/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.table.runtime.operators.aggregate.async.queue.KeyedAsyncOutputMode;

/**
 * The factory of {@link KeyedAsyncWaitOperator}.
 *
 * @param <OUT> The output type of the operator
 */
public class KeyedAsyncWaitOperatorFactory<K, IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>, YieldingOperatorFactory<OUT> {
    private final KeyedAsyncFunction<K, IN, OUT> asyncFunction;
    private final long timeout;
    private final int capacity;
    private final KeyedAsyncOutputMode outputMode;

    public KeyedAsyncWaitOperatorFactory(
            KeyedAsyncFunction<K, IN, OUT> asyncFunction,
            long timeout,
            int capacity,
            KeyedAsyncOutputMode outputMode) {
        this.asyncFunction = asyncFunction;
        this.timeout = timeout;
        this.capacity = capacity;
        this.outputMode = outputMode;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        KeyedAsyncWaitOperator<K, IN, OUT> asyncWaitOperator =
                new KeyedAsyncWaitOperator<>(
                        asyncFunction,
                        timeout,
                        capacity,
                        outputMode,
                        processingTimeService,
                        getMailboxExecutor());
        asyncWaitOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) asyncWaitOperator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AsyncWaitOperator.class;
    }
}
