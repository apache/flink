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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * A {@link Transformation} that describes a reduce operation on a {@link KeyedStream}.
 *
 * @param <IN> The input and output type of the transformation.
 * @param <K> The type of the key of the stream.
 */
@Internal
public final class ReduceTransformation<IN, K> extends PhysicalTransformation<IN> {
    private final Transformation<IN> input;
    private final ReduceFunction<IN> reducer;
    private final KeySelector<IN, K> keySelector;
    private final TypeInformation<K> keyTypeInfo;
    private ChainingStrategy chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;

    public ReduceTransformation(
            String name,
            int parallelism,
            Transformation<IN> input,
            ReduceFunction<IN> reducer,
            KeySelector<IN, K> keySelector,
            TypeInformation<K> keyTypeInfo) {
        super(name, input.getOutputType(), parallelism);
        this.input = input;
        this.reducer = reducer;
        this.keySelector = keySelector;
        this.keyTypeInfo = keyTypeInfo;

        updateManagedMemoryStateBackendUseCase(true);
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        this.chainingStrategy = strategy;
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }

    public KeySelector<IN, K> getKeySelector() {
        return keySelector;
    }

    public TypeInformation<K> getKeyTypeInfo() {
        return keyTypeInfo;
    }

    public ReduceFunction<IN> getReducer() {
        return reducer;
    }

    /** Returns the {@code TypeInformation} for the elements of the input. */
    public TypeInformation<IN> getInputType() {
        return input.getOutputType();
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }
}
