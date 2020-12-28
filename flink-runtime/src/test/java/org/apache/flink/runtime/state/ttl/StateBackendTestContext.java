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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;

/** Base class for state backend test context. */
public abstract class StateBackendTestContext {
    public static final int NUMBER_OF_KEY_GROUPS = 10;

    private final StateBackend stateBackend;
    private final CheckpointOptions checkpointOptions;
    private final CheckpointStreamFactory checkpointStreamFactory;
    private final TtlTimeProvider timeProvider;
    private final SharedStateRegistry sharedStateRegistry;
    private final List<KeyedStateHandle> snapshots;

    private MockEnvironment env;

    private CheckpointableKeyedStateBackend<String> keyedStateBackend;

    protected StateBackendTestContext(TtlTimeProvider timeProvider) {
        this.timeProvider = Preconditions.checkNotNull(timeProvider);
        this.stateBackend = Preconditions.checkNotNull(createStateBackend());
        this.checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
        this.checkpointStreamFactory = createCheckpointStreamFactory();
        this.sharedStateRegistry = new SharedStateRegistry();
        this.snapshots = new ArrayList<>();
    }

    protected abstract StateBackend createStateBackend();

    private CheckpointStreamFactory createCheckpointStreamFactory() {
        try {
            return stateBackend
                    .createCheckpointStorage(new JobID())
                    .resolveCheckpointStorageLocation(2L, checkpointOptions.getTargetLocation());
        } catch (IOException e) {
            throw new RuntimeException("unexpected");
        }
    }

    void createAndRestoreKeyedStateBackend(KeyedStateHandle snapshot) {
        createAndRestoreKeyedStateBackend(NUMBER_OF_KEY_GROUPS, snapshot);
    }

    void createAndRestoreKeyedStateBackend(int numberOfKeyGroups, KeyedStateHandle snapshot) {
        Collection<KeyedStateHandle> stateHandles;
        if (snapshot == null) {
            stateHandles = Collections.emptyList();
        } else {
            stateHandles = new ArrayList<>(1);
            stateHandles.add(snapshot);
        }
        env = MockEnvironment.builder().build();
        try {
            disposeKeyedStateBackend();
            keyedStateBackend =
                    stateBackend.createKeyedStateBackend(
                            env,
                            new JobID(),
                            "test",
                            StringSerializer.INSTANCE,
                            numberOfKeyGroups,
                            new KeyGroupRange(0, numberOfKeyGroups - 1),
                            env.getTaskKvStateRegistry(),
                            timeProvider,
                            new UnregisteredMetricsGroup(),
                            stateHandles,
                            new CloseableRegistry());
        } catch (Exception e) {
            throw new RuntimeException("unexpected", e);
        }
    }

    void dispose() throws Exception {
        disposeKeyedStateBackend();
        for (KeyedStateHandle snapshot : snapshots) {
            snapshot.discardState();
        }
        snapshots.clear();
        sharedStateRegistry.close();
        if (env != null) {
            env.close();
        }
    }

    private void disposeKeyedStateBackend() {
        if (keyedStateBackend != null) {
            keyedStateBackend.dispose();
            keyedStateBackend = null;
        }
    }

    KeyedStateHandle takeSnapshot() throws Exception {
        SnapshotResult<KeyedStateHandle> snapshotResult = triggerSnapshot().get();
        KeyedStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();
        if (jobManagerOwnedSnapshot != null) {
            jobManagerOwnedSnapshot.registerSharedStates(sharedStateRegistry);
        }
        return jobManagerOwnedSnapshot;
    }

    @Nonnull
    RunnableFuture<SnapshotResult<KeyedStateHandle>> triggerSnapshot() throws Exception {
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotRunnableFuture =
                keyedStateBackend.snapshot(
                        682375462392L, 10L, checkpointStreamFactory, checkpointOptions);
        if (!snapshotRunnableFuture.isDone()) {
            snapshotRunnableFuture.run();
        }
        return snapshotRunnableFuture;
    }

    public void setCurrentKey(String key) {
        //noinspection resource
        Preconditions.checkNotNull(keyedStateBackend, "keyed backend is not initialised");
        keyedStateBackend.setCurrentKey(key);
    }

    @SuppressWarnings("unchecked")
    <N, S extends State, V> S createState(
            StateDescriptor<S, V> stateDescriptor,
            @SuppressWarnings("SameParameterValue") N defaultNamespace)
            throws Exception {
        S state =
                keyedStateBackend.getOrCreateKeyedState(StringSerializer.INSTANCE, stateDescriptor);
        ((InternalKvState<?, N, ?>) state).setCurrentNamespace(defaultNamespace);
        return state;
    }

    @SuppressWarnings("unchecked")
    public <B extends CheckpointableKeyedStateBackend<String>> B getKeyedStateBackend() {
        return (B) keyedStateBackend;
    }
}
