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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.io.IOException;

/** A {@link ReducingState} which keeps value for a single key at a time. */
class BatchExecutionKeyReducingState<K, N, T>
        extends MergingAbstractBatchExecutionKeyState<K, N, T, T, T>
        implements InternalReducingState<K, N, T> {
    private final ReduceFunction<T> reduceFunction;

    public BatchExecutionKeyReducingState(
            T defaultValue,
            ReduceFunction<T> reduceFunction,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<T> stateSerializer) {
        super(defaultValue, keySerializer, namespaceSerializer, stateSerializer);
        this.reduceFunction = reduceFunction;
    }

    @Override
    public T get() {
        return getOrDefault();
    }

    @Override
    public void add(T value) throws IOException {
        if (value == null) {
            clear();
            return;
        }

        try {
            T currentNamespaceValue = getCurrentNamespaceValue();
            if (currentNamespaceValue != null) {
                setCurrentNamespaceValue(reduceFunction.reduce(currentNamespaceValue, value));
            } else {
                setCurrentNamespaceValue(value);
            }
        } catch (Exception e) {
            throw new IOException("Exception while applying ReduceFunction in reducing state", e);
        }
    }

    @SuppressWarnings("unchecked")
    static <T, K, N, SV, S extends State, IS extends S> IS create(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc) {
        return (IS)
                new BatchExecutionKeyReducingState<>(
                        stateDesc.getDefaultValue(),
                        ((ReducingStateDescriptor<SV>) stateDesc).getReduceFunction(),
                        keySerializer,
                        namespaceSerializer,
                        stateDesc.getSerializer());
    }

    @Override
    protected T merge(T target, T source) throws Exception {
        return reduceFunction.reduce(target, source);
    }
}
