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
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalReducingState;

/**
 * An adaptor that transforms {@link org.apache.flink.runtime.state.internal.InternalReducingState}
 * into {@link org.apache.flink.runtime.state.v2.internal.InternalReducingState}.
 */
public class ReducingStateAdaptor<K, N, V> extends MergingStateAdaptor<K, N, V, V, V, V>
        implements InternalReducingState<K, N, V> {

    public ReducingStateAdaptor(
            org.apache.flink.runtime.state.internal.InternalReducingState<K, N, V> reducingState) {
        super(reducingState);
    }

    @Override
    public StateFuture<V> asyncGet() {
        try {
            return StateFutureUtils.completedFuture(delegatedState.get());
        } catch (Exception e) {
            throw new RuntimeException("Error while get value from raw ReducingState", e);
        }
    }

    @Override
    public StateFuture<V> asyncGetInternal() {
        try {
            return StateFutureUtils.completedFuture(delegatedState.getInternal());
        } catch (Exception e) {
            throw new RuntimeException("Error while get value from raw ReducingState", e);
        }
    }

    @Override
    public StateFuture<Void> asyncUpdateInternal(V valueToStore) {
        try {
            delegatedState.updateInternal(valueToStore);
            return StateFutureUtils.completedVoidFuture();
        } catch (Exception e) {
            throw new RuntimeException("Error while update value to raw ReducingState", e);
        }
    }

    @Override
    public V getInternal() {
        try {
            return delegatedState.getInternal();
        } catch (Exception e) {
            throw new RuntimeException("Error while get internal value from raw ReducingState", e);
        }
    }

    @Override
    public void updateInternal(V valueToStore) {
        try {
            delegatedState.updateInternal(valueToStore);
        } catch (Exception e) {
            throw new RuntimeException("Error while update internal value to raw ReducingState", e);
        }
    }
}
