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

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

/**
 * An base implementation of state adaptor from v1 to v2.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <S> The type of delegated state
 */
public class StateAdaptor<K, N, S extends InternalKvState<K, N, ?>>
        implements InternalPartitionedState<N>, State {

    final S delegatedState;

    StateAdaptor(S delegatedState) {
        this.delegatedState = delegatedState;
    }

    @Override
    public StateFuture<Void> asyncClear() {
        delegatedState.clear();
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public void clear() {
        delegatedState.clear();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        delegatedState.setCurrentNamespace(namespace);
    }
}
