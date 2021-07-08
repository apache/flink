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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;

/**
 * An abstract class with a common implementation of {@link
 * InternalMergingState#mergeNamespaces(Object, Collection)}.
 */
abstract class MergingAbstractBatchExecutionKeyState<K, N, V, IN, OUT>
        extends AbstractBatchExecutionKeyState<K, N, V>
        implements InternalMergingState<K, N, IN, V, OUT> {
    protected MergingAbstractBatchExecutionKeyState(
            V defaultValue,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> stateTypeSerializer) {
        super(defaultValue, keySerializer, namespaceSerializer, stateTypeSerializer);
    }

    protected abstract V merge(V target, V source) throws Exception;

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        setCurrentNamespace(target);
        V targetValue = getCurrentNamespaceValue();
        for (N source : sources) {
            setCurrentNamespace(source);
            V sourceValue = getCurrentNamespaceValue();
            if (targetValue == null) {
                targetValue = sourceValue;
            } else if (sourceValue != null) {
                targetValue = merge(targetValue, sourceValue);
                clear();
            }
        }
        setCurrentNamespace(target);
        setCurrentNamespaceValue(targetValue);
    }

    @Override
    public V getInternal() {
        return getCurrentNamespaceValue();
    }

    @Override
    public void updateInternal(V valueToStore) {
        setCurrentNamespaceValue(valueToStore);
    }
}
