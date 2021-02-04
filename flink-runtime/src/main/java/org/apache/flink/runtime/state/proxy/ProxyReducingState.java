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

package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.util.Collection;

/** */
public class ProxyReducingState<K, N, V> implements InternalReducingState<K, N, V> {
    InternalReducingState<K, N, V> reducingState;

    ProxyReducingState(InternalReducingState<K, N, V> internalReducingState) {
        this.reducingState = internalReducingState;
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        reducingState.mergeNamespaces(target, sources);
    }

    @Override
    public V getInternal() throws Exception {
        return reducingState.getInternal();
    }

    @Override
    public void updateInternal(V valueToStore) throws Exception {
        reducingState.updateInternal(valueToStore);
    }

    @Override
    public V get() throws Exception {
        return reducingState.get();
    }

    @Override
    public void add(V value) throws Exception {
        reducingState.add(value);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return reducingState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return reducingState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return reducingState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        reducingState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        return reducingState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return reducingState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public void clear() {
        reducingState.clear();
    }

    @SuppressWarnings("unchecked")
    static <E, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> reducingState) {
        return (IS) new ProxyReducingState<>((InternalReducingState<K, N, SV>) reducingState);
    }
}
