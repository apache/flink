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
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.util.Collection;

/** */
public class ProxyAggregatingState<K, N, IN, ACC, OUT>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {
    private final InternalAggregatingState<K, N, IN, ACC, OUT> aggregatingState;

    ProxyAggregatingState(InternalAggregatingState<K, N, IN, ACC, OUT> aggregatingState) {
        this.aggregatingState = aggregatingState;
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        aggregatingState.mergeNamespaces(target, sources);
    }

    @Override
    public ACC getInternal() throws Exception {
        return aggregatingState.getInternal();
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        aggregatingState.updateInternal(valueToStore);
    }

    @Override
    public OUT get() throws Exception {
        return aggregatingState.get();
    }

    @Override
    public void add(IN value) throws Exception {
        aggregatingState.add(value);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return aggregatingState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return aggregatingState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<ACC> getValueSerializer() {
        return aggregatingState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        aggregatingState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<ACC> safeValueSerializer)
            throws Exception {
        return aggregatingState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, ACC> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return aggregatingState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public void clear() {
        aggregatingState.clear();
    }

    @SuppressWarnings("unchecked")
    static <T, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> aggregatingState) {
        return (IS)
                new ProxyAggregatingState<>(
                        (InternalAggregatingState<K, N, T, SV, ?>) aggregatingState);
    }
}
