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

package org.apache.flink.runtime.state.testutils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A test backends that allows you to supply a specific test stream. */
@SuppressWarnings({"serial"})
public class BackendForTestStream extends MemoryStateBackend {

    private static final long serialVersionUID = 1L;

    private final TestFactory streamFactory;

    public BackendForTestStream(TestFactory streamFactory) {
        this.streamFactory = checkNotNull(streamFactory);
    }

    public BackendForTestStream(StreamFactory streamSupplier) {
        this(new TestFactory(streamSupplier));
    }

    // make no reconfiguration!
    @Override
    public MemoryStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
        return this;
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(JobID jobId) {
        return new TestCheckpointStorageAccess();
    }

    // ------------------------------------------------------------------------

    public interface StreamFactory
            extends SupplierWithException<CheckpointStateOutputStream, IOException>,
                    java.io.Serializable {}

    // ------------------------------------------------------------------------

    private final class TestCheckpointStorageAccess implements CheckpointStorageAccess {

        @Override
        public boolean supportsHighlyAvailableStorage() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasDefaultSavepointLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initializeBaseLocationsForCheckpoint() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStorageLocation initializeLocationForSavepoint(
                long checkpointId, @Nullable String externalLocationPointer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId, CheckpointStorageLocationReference reference) {
            return streamFactory;
        }

        @Override
        public CheckpointStateOutputStream createTaskOwnedStateStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestFactory
            implements CheckpointStreamFactory, java.io.Serializable {

        private final StreamFactory streamFactory;

        TestFactory(StreamFactory streamFactory) {
            this.streamFactory = streamFactory;
        }

        @Override
        public CheckpointStateOutputStream createCheckpointStateOutputStream(
                CheckpointedStateScope scope) throws IOException {
            return streamFactory.get();
        }

        @Override
        public boolean canFastDuplicate(
                StreamStateHandle stateHandle, CheckpointedStateScope scope) {
            return false;
        }

        @Override
        public List<StreamStateHandle> duplicate(
                List<StreamStateHandle> stateHandles, CheckpointedStateScope scope)
                throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
