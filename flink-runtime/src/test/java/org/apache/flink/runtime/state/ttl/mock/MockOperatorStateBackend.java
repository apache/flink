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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

class MockOperatorStateBackend implements OperatorStateBackend {

    private final HashSet<String> registeredStateNames = new HashSet<>();
    private final boolean emptySnapshot;

    public MockOperatorStateBackend(boolean emptySnapshot) {
        this.emptySnapshot = emptySnapshot;
    }

    @Override
    public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
        registeredStateNames.add(stateDescriptor.getName());
        ListState<S> state =
                MockInternalListState.createState(
                        stateDescriptor.getElementSerializer(), stateDescriptor);
        ((MockInternalKvState) state).values = HashMap::new;
        return state;
    }

    @Override
    public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor)
            throws Exception {
        return getListState(stateDescriptor);
    }

    @Override
    public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getRegisteredStateNames() {
        return registeredStateNames;
    }

    @Override
    public Set<String> getRegisteredBroadcastStateNames() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        if (!emptySnapshot) {
            throw new UnsupportedOperationException();
        }
        return new FutureTask<>(SnapshotResult::empty);
    }

    @Override
    public void dispose() {}

    @Override
    public void close() throws IOException {}
}
