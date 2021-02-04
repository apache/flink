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
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/** */
public class ProxyMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV> {
    private final InternalMapState<K, N, UK, UV> mapState;

    ProxyMapState(InternalMapState<K, N, UK, UV> mapState) {
        this.mapState = mapState;
    }

    @Override
    public UV get(UK key) throws Exception {
        return mapState.get(key);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        mapState.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        mapState.putAll(map);
    }

    @Override
    public void remove(UK key) throws Exception {
        mapState.remove(key);
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return mapState.contains(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        return mapState.entries();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return mapState.keys();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return mapState.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return mapState.iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        return mapState.isEmpty();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return mapState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return mapState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return mapState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        mapState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {
        return mapState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return mapState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public void clear() {
        mapState.clear();
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> mapState) {
        return (IS) new ProxyMapState<>((InternalMapState<K, N, UK, UV>) mapState);
    }
}
