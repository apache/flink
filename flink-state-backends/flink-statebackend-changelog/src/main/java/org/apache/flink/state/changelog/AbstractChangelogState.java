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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for changelog state wrappers of state objects.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> The type of values kept internally in state without changelog wrapper
 * @param <S> Type of originally wrapped state object
 */
abstract class AbstractChangelogState<K, N, V, S extends InternalKvState<K, N, V>>
        implements InternalKvState<K, N, V>, ChangelogState {

    protected final S delegatedState;
    protected final KvStateChangeLogger<V, N> changeLogger;
    private N currentNamespace;

    AbstractChangelogState(S state, KvStateChangeLogger<V, N> changeLogger) {
        checkArgument(!(state instanceof AbstractChangelogState));
        this.delegatedState = checkNotNull(state);
        this.changeLogger = checkNotNull(changeLogger);
    }

    public S getDelegatedState() {
        return delegatedState;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegatedState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegatedState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return delegatedState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        currentNamespace = namespace;
        delegatedState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        return delegatedState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return delegatedState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    protected N getCurrentNamespace() throws NullPointerException {
        return checkNotNull(currentNamespace);
    }
}
