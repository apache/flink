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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.state.changelog.StateChangeOperation;

import java.util.ArrayList;
import java.util.Collection;

abstract class KvStateChangeApplier<K, N> implements StateChangeApplier {
    private final InternalKeyContext<K> keyContext;

    protected abstract InternalKvState<K, N, ?> getState();

    protected KvStateChangeApplier(InternalKeyContext<K> keyContext) {
        this.keyContext = keyContext;
    }

    @Override
    public void apply(StateChangeOperation operation, DataInputView in) throws Exception {
        K key = getState().getKeySerializer().deserialize(in);
        keyContext.setCurrentKey(key);
        keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups()));
        getState().setCurrentNamespace(getState().getNamespaceSerializer().deserialize(in));
        applyInternal(operation, in);
    }

    protected abstract void applyInternal(StateChangeOperation operation, DataInputView in)
            throws Exception;

    protected static <K, N, T> void applyMergeNamespaces(
            InternalMergingState<K, N, T, ?, ?> state, DataInputView in) throws Exception {
        N target = state.getNamespaceSerializer().deserialize(in);
        int sourcesSize = in.readInt();
        Collection<N> sources = new ArrayList<>(sourcesSize);
        for (int i = 0; i < sourcesSize; i++) {
            sources.add(state.getNamespaceSerializer().deserialize(in));
        }
        state.mergeNamespaces(target, sources);
    }
}
