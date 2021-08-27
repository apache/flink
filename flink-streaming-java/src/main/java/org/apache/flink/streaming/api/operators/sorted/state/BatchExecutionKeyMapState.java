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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** A {@link MapState} which keeps value for a single key at a time. */
class BatchExecutionKeyMapState<K, N, UK, UV>
        extends AbstractBatchExecutionKeyState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    protected BatchExecutionKeyMapState(
            Map<UK, UV> defaultValue,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<Map<UK, UV>> stateTypeSerializer) {
        super(defaultValue, keySerializer, namespaceSerializer, stateTypeSerializer);
    }

    @Override
    public UV get(UK key) throws Exception {
        if (getCurrentNamespaceValue() == null) {
            return null;
        }
        return getCurrentNamespaceValue().get(key);
    }

    @Override
    public void put(UK key, UV value) {
        initIfNull();
        getCurrentNamespaceValue().put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) {
        initIfNull();
        this.getCurrentNamespaceValue().putAll(map);
    }

    private void initIfNull() {
        if (getCurrentNamespaceValue() == null) {
            setCurrentNamespaceValue(new HashMap<>());
        }
    }

    @Override
    public void remove(UK key) throws Exception {
        if (getCurrentNamespaceValue() == null) {
            return;
        }
        getCurrentNamespaceValue().remove(key);
        if (getCurrentNamespaceValue().isEmpty()) {
            clear();
        }
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return getCurrentNamespaceValue() != null && getCurrentNamespaceValue().containsKey(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        return getCurrentNamespaceValue() == null
                ? Collections.emptySet()
                : getCurrentNamespaceValue().entrySet();
    }

    @Override
    public Iterable<UK> keys() {
        return getCurrentNamespaceValue() == null
                ? Collections.emptySet()
                : getCurrentNamespaceValue().keySet();
    }

    @Override
    public Iterable<UV> values() {
        return getCurrentNamespaceValue() == null
                ? Collections.emptySet()
                : getCurrentNamespaceValue().values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        return getCurrentNamespaceValue() == null
                ? Collections.emptyIterator()
                : getCurrentNamespaceValue().entrySet().iterator();
    }

    @Override
    public boolean isEmpty() {
        return getCurrentNamespaceValue() == null || getCurrentNamespaceValue().isEmpty();
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc) {
        return (IS)
                new BatchExecutionKeyMapState<>(
                        (Map<UK, UV>) stateDesc.getDefaultValue(),
                        keySerializer,
                        namespaceSerializer,
                        (TypeSerializer<Map<UK, UV>>) stateDesc.getSerializer());
    }
}
