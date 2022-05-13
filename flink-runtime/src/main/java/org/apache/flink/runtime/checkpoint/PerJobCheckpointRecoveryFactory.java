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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Simple {@link CheckpointRecoveryFactory} which creates and keeps separate {@link
 * CompletedCheckpointStore} and {@link CheckpointIDCounter} for each {@link JobID}.
 */
public class PerJobCheckpointRecoveryFactory<T extends CompletedCheckpointStore>
        implements CheckpointRecoveryFactory {

    @VisibleForTesting
    public static <T extends CompletedCheckpointStore>
            CheckpointRecoveryFactory withoutCheckpointStoreRecovery(IntFunction<T> storeFn) {
        return new PerJobCheckpointRecoveryFactory<>(
                (maxCheckpoints, previous, sharedStateRegistry, ioExecutor, restoreMode) -> {
                    if (previous != null) {
                        throw new UnsupportedOperationException(
                                "Checkpoint store recovery is not supported.");
                    }
                    return storeFn.apply(maxCheckpoints);
                });
    }

    private final CheckpointStoreRecoveryHelper<T> checkpointStoreRecoveryHelper;
    private final Supplier<CheckpointIDCounter> checkpointIDCounterPerJobFactory;
    private final ConcurrentMap<JobID, T> store;
    private final ConcurrentMap<JobID, CheckpointIDCounter> counter;

    public PerJobCheckpointRecoveryFactory(
            CheckpointStoreRecoveryHelper<T> checkpointStoreRecoveryHelper) {
        this(checkpointStoreRecoveryHelper, StandaloneCheckpointIDCounter::new);
    }

    public PerJobCheckpointRecoveryFactory(
            CheckpointStoreRecoveryHelper<T> checkpointStoreRecoveryHelper,
            Supplier<CheckpointIDCounter> checkpointIDCounterPerJobFactory) {
        this.checkpointIDCounterPerJobFactory = checkpointIDCounterPerJobFactory;
        this.store = new ConcurrentHashMap<>();
        this.counter = new ConcurrentHashMap<>();
        this.checkpointStoreRecoveryHelper = checkpointStoreRecoveryHelper;
    }

    @Override
    public CompletedCheckpointStore createRecoveredCompletedCheckpointStore(
            JobID jobId,
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            RestoreMode restoreMode) {
        return store.compute(
                jobId,
                (key, previous) ->
                        checkpointStoreRecoveryHelper.recoverCheckpointStore(
                                maxNumberOfCheckpointsToRetain,
                                previous,
                                sharedStateRegistryFactory,
                                ioExecutor,
                                restoreMode));
    }

    @Override
    public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) {
        return counter.computeIfAbsent(jobId, jId -> checkpointIDCounterPerJobFactory.get());
    }

    /** Restores or creates a {@link CompletedCheckpointStore}, optionally using an existing one. */
    @Internal
    public interface CheckpointStoreRecoveryHelper<StoreType extends CompletedCheckpointStore> {
        StoreType recoverCheckpointStore(
                int maxNumberOfCheckpointsToRetain,
                @Nullable StoreType previousStore,
                SharedStateRegistryFactory sharedStateRegistryFactory,
                Executor ioExecutor,
                RestoreMode restoreMode);
    }
}
