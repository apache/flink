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

package org.apache.flink.state.hashmap;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy.MetadataWriter.MetadataWriterImpl;
import org.apache.flink.runtime.state.heap.StateSnapshotWriter;
import org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationRunnable;
import org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationTarget;
import org.apache.flink.state.hashmap.IncrementalSnapshot.Versions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.AlignmentType.ALIGNED;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.NO_ALIGNED_CHECKPOINT_TIME_OUT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.FULL_CHECKPOINT;

class HeapBackendMaterializationTarget implements MaterializationTarget<Versions> {
    private static final Logger LOG =
            LoggerFactory.getLogger(HeapBackendMaterializationTarget.class);

    private static final CheckpointOptions CHECKPOINT_OPTIONS =
            new CheckpointOptions(
                    FULL_CHECKPOINT,
                    CheckpointStorageLocationReference.getDefault(),
                    ALIGNED,
                    NO_ALIGNED_CHECKPOINT_TIME_OUT);

    private final IncrementalHeapSnapshotStrategy<?> snapshotStrategy;
    private final IncrementalSnapshotTracker snapshotTracker;
    private final CloseableRegistry closeableRegistry;

    private final KeyGroupRange keyGroupRange;
    private final StreamCompressionDecorator compressionDecorator;
    private final LocalRecoveryConfig localRecoveryConfig;
    private final CheckpointStreamFactory streamFactory;

    private long lastStartedMaterializationID = -1L;
    private long lastCompletedMaterializationID = -1L;

    public HeapBackendMaterializationTarget(
            IncrementalHeapSnapshotStrategy<?> snapshotStrategy,
            IncrementalSnapshotTracker snapshotTracker,
            CloseableRegistry closeableRegistry,
            KeyGroupRange keyGroupRange,
            StreamCompressionDecorator compressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            CheckpointStreamFactory streamFactory) {
        this.snapshotStrategy = snapshotStrategy;
        this.snapshotTracker = snapshotTracker;
        this.closeableRegistry = closeableRegistry;
        this.keyGroupRange = keyGroupRange;
        this.compressionDecorator = compressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.streamFactory = streamFactory;
    }

    @Override
    public Optional<MaterializationRunnable<Versions>> initMaterialization() throws Exception {

        final long materializationID = ++this.lastStartedMaterializationID;
        LOG.info("start materialization {}", materializationID);

        final IncrementalHeapSnapshotResources<?> syncResources =
                snapshotStrategy.syncPrepareResources(materializationID);

        return Optional.of(
                new MaterializationRunnable<>(
                        createAsyncPhase(syncResources, materializationID),
                        materializationID,
                        new Versions(syncResources.getCurrentMapVersions())));
    }

    @Override
    public void handleMaterializationResult(
            SnapshotResult<KeyedStateHandle> snapshot, long materializationID, Versions versions) {
        if (materializationID > lastCompletedMaterializationID) {
            LOG.info("materialization {} completed", materializationID);
            lastCompletedMaterializationID = materializationID;
            snapshotTracker.trackFullSnapshot(snapshot, versions);
        } else {
            LOG.warn("materialization {} completed out of order - ignoring", materializationID);
        }
    }

    private RunnableFuture<SnapshotResult<KeyedStateHandle>> createAsyncPhase(
            IncrementalHeapSnapshotResources<?> snapshotResources, long materializationID)
            throws IOException {

        HeapSnapshotStrategy.HeapSnapshotWriter<?> snapshotWriter =
                new HeapSnapshotStrategy.HeapSnapshotWriter<>(
                        snapshotResources,
                        keyGroupRange,
                        compressionDecorator,
                        StateSnapshotWriter.DEFAULT,
                        localRecoveryConfig,
                        CHECKPOINT_OPTIONS,
                        // todo: check how it interacts with
                        // CheckpointStorageWorkerView#createTaskOwnedStateStream
                        // which is passed here via constructor
                        CheckpointedStateScope.SHARED,
                        materializationID,
                        streamFactory,
                        new MetadataWriterImpl<>(snapshotResources, compressionDecorator));

        return new AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>>() {
            @Override
            protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {
                return snapshotWriter.get(snapshotCloseableRegistry);
            }

            @Override
            protected void cleanupProvidedResources() {
                snapshotResources.release();
            }
        }.toAsyncSnapshotFutureTask(closeableRegistry);
    }
}
