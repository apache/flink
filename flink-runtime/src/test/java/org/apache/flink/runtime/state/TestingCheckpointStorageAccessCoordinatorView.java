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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/** A testing implementation of the {@link CheckpointStorageCoordinatorView}. */
@SuppressWarnings("serial")
public class TestingCheckpointStorageAccessCoordinatorView
        implements CheckpointStorageAccess, java.io.Serializable {

    private final HashMap<String, TestingCompletedCheckpointStorageLocation> registeredSavepoints =
            new HashMap<>();

    // ------------------------------------------------------------------------
    //  test setup methods
    // ------------------------------------------------------------------------

    public void registerSavepoint(String pointer, byte[] metadata) {
        registeredSavepoints.put(
                pointer, new TestingCompletedCheckpointStorageLocation(pointer, metadata));
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorageCoordinatorView methods
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsHighlyAvailableStorage() {
        return false;
    }

    @Override
    public boolean hasDefaultSavepointLocation() {
        return false;
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
            throws IOException {
        final CompletedCheckpointStorageLocation location =
                registeredSavepoints.get(externalPointer);
        if (location != null) {
            return location;
        } else {
            throw new IOException("Could not find savepoint for pointer: " + externalPointer);
        }
    }

    @Override
    public void initializeBaseLocations() throws IOException {}

    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
            throws IOException {
        return new NonPersistentMetadataCheckpointStorageLocation(Integer.MAX_VALUE);
    }

    @Override
    public CheckpointStorageLocation initializeLocationForSavepoint(
            long checkpointId, @Nullable String externalLocationPointer) throws IOException {
        return new NonPersistentMetadataCheckpointStorageLocation(Integer.MAX_VALUE);
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) {
        return new MemCheckpointStreamFactory(Integer.MAX_VALUE);
    }

    @Override
    public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() {
        return new MemCheckpointStreamFactory.MemoryCheckpointOutputStream(Integer.MAX_VALUE);
    }

    // ------------------------------------------------------------------------
    //  Utils
    // ------------------------------------------------------------------------

    public StateBackend asStateBackend() {
        return new FactoringStateBackend(this);
    }

    // ------------------------------------------------------------------------
    //  internal support classes
    // ------------------------------------------------------------------------

    private static final class TestingCompletedCheckpointStorageLocation
            implements CompletedCheckpointStorageLocation, java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final String pointer;
        private final byte[] metadata;

        TestingCompletedCheckpointStorageLocation(String pointer, byte[] metadata) {
            this.pointer = pointer;
            this.metadata = metadata;
        }

        @Override
        public String getExternalPointer() {
            return pointer;
        }

        @Override
        public StreamStateHandle getMetadataHandle() {
            return new ByteStreamStateHandle(pointer, metadata);
        }

        @Override
        public void disposeStorageLocation() throws IOException {}
    }

    // ------------------------------------------------------------------------
    //   Everything below here is necessary only to make it possible to
    //   pass the CheckpointStorageCoordinatorView to the CheckpointCoordinator
    //   via the JobGraph, because that part expects a StateBackend
    // ------------------------------------------------------------------------

    /** A StateBackend whose only purpose is to create a given CheckpointStorage. */
    private static final class FactoringStateBackend implements StateBackend {

        private final TestingCheckpointStorageAccessCoordinatorView testingCoordinatorView;

        private FactoringStateBackend(
                TestingCheckpointStorageAccessCoordinatorView testingCoordinatorView) {
            this.testingCoordinatorView = testingCoordinatorView;
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
                throws IOException {
            return testingCoordinatorView.resolveCheckpoint(externalPointer);
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return testingCoordinatorView;
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            throw new UnsupportedOperationException();
        }
    }
}
