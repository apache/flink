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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * The factory of Keyed Async Wait Operator.
 *
 * @param <IN> The input type of the operator
 * @param <OUT> The output type of the operator
 * @param <KEY> The key type of the operator
 */
public class TableKeyedAsyncWaitOperatorFactory<IN, OUT, KEY>
        extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT> {

    private final AsyncFunction<IN, OUT> asyncFunction;
    private final KeySelector<IN, KEY> keySelector;
    private final long timeout;
    private final int capacity;

    public TableKeyedAsyncWaitOperatorFactory(
            AsyncFunction<IN, OUT> asyncFunction,
            KeySelector<IN, KEY> keySelector,
            long timeout,
            int capacity) {
        this.asyncFunction = asyncFunction;
        this.keySelector = keySelector;
        this.timeout = timeout;
        this.capacity = capacity;
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        return null;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AbstractStreamOperator.class;
    }
}
