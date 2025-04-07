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

package org.apache.flink.runtime.asyncprocessing.declare.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.state.v2.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.runtime.state.v2.internal.InternalMapState;
import org.apache.flink.runtime.state.v2.internal.InternalReducingState;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * A partitioned state that wraps a declared namespace and hide the namespace switching from user.
 * User will only use the state just like the public state APIs without any consideration on
 * namespace.
 *
 * <p>This wrap class is useful in DataStream window operation, where namespace is managed by the
 * operator and user function is free from namespace manipulation.
 */
@Internal
public abstract class StateWithDeclaredNamespace<K, N, V> implements InternalKeyedState<K, N, V> {
    @Nonnull private final InternalKeyedState<K, N, V> state;
    @Nonnull private final DeclaredVariable<N> declaredNamespace;

    public StateWithDeclaredNamespace(
            @Nonnull InternalKeyedState<K, N, V> state,
            @Nonnull DeclaredVariable<N> declaredNamespace) {
        Preconditions.checkNotNull(state);
        Preconditions.checkNotNull(declaredNamespace);

        this.state = state;
        this.declaredNamespace = declaredNamespace;
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        declaredNamespace.set(namespace);
        state.setCurrentNamespace(namespace);
    }

    /** Automatically called before any async state access. */
    protected void resetNamespace() {
        state.setCurrentNamespace(declaredNamespace.get());
    }

    @SuppressWarnings("unchecked")
    public static <N, S extends State> S create(S state, DeclaredVariable<N> declaredNamespace) {
        if (state instanceof InternalReducingState) {
            return (S)
                    new ReducingStateWithDeclaredNamespace<>(
                            (InternalReducingState<?, N, ?>) state, declaredNamespace);
        } else if (state instanceof InternalAggregatingState) {
            return (S)
                    new AggregatingStateWithDeclaredNamespace<>(
                            (InternalAggregatingState<?, N, ?, ?, ?>) state, declaredNamespace);
        } else if (state instanceof InternalValueState) {
            return (S)
                    new ValueStateWithDeclaredNamespace<>(
                            (InternalValueState<?, N, ?>) state, declaredNamespace);
        } else if (state instanceof InternalMapState) {
            return (S)
                    new MapStateWithDeclaredNamespace<>(
                            (InternalMapState<?, N, ?, ?>) state, declaredNamespace);
        } else if (state instanceof InternalListState) {
            return (S)
                    new ListStateWithDeclaredNamespace<>(
                            (InternalListState<?, N, ?>) state, declaredNamespace);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported state type: " + state.getClass().getName());
        }
    }
}
