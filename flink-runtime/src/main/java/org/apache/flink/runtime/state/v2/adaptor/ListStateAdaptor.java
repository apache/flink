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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalListState;

import java.util.List;

/**
 * An adaptor that transforms {@link org.apache.flink.runtime.state.internal.InternalListState} into
 * {@link org.apache.flink.runtime.state.v2.internal.InternalListState}.
 */
public class ListStateAdaptor<K, N, V>
        extends StateAdaptor<
                K, N, org.apache.flink.runtime.state.internal.InternalListState<K, N, V>>
        implements InternalListState<K, N, V> {

    public ListStateAdaptor(
            org.apache.flink.runtime.state.internal.InternalListState<K, N, V> listState) {
        super(listState);
    }

    @Override
    public StateFuture<Void> asyncUpdate(List<V> values) {
        try {
            delegatedState.update(values);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating values to raw ListState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void update(List<V> values) {
        try {
            delegatedState.update(values);
        } catch (Exception e) {
            throw new RuntimeException("Error while updating values to raw ListState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncAddAll(List<V> values) {
        try {
            delegatedState.addAll(values);
        } catch (Exception e) {
            throw new RuntimeException("Error while adding values to raw ListState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void addAll(List<V> values) {
        try {
            delegatedState.addAll(values);
        } catch (Exception e) {
            throw new RuntimeException("Error while adding values to raw ListState", e);
        }
    }

    @Override
    public StateFuture<StateIterator<V>> asyncGet() {
        try {
            return StateFutureUtils.completedFuture(
                    new CompleteStateIterator<>(delegatedState.get()));
        } catch (Exception e) {
            throw new RuntimeException("Error while getting values from raw ListState", e);
        }
    }

    @Override
    public Iterable<V> get() {
        try {
            return delegatedState.get();
        } catch (Exception e) {
            throw new RuntimeException("Error while getting values from raw ListState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncAdd(V value) {
        try {
            delegatedState.add(value);
        } catch (Exception e) {
            throw new RuntimeException("Error while adding value to raw ListState", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void add(V value) {
        try {
            delegatedState.add(value);
        } catch (Exception e) {
            throw new RuntimeException("Error while adding value to raw ListState", e);
        }
    }
}
