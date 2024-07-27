/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

import static org.mockito.Mockito.spy;

/**
 * This class wraps an {@link AbstractStateBackend} and enriches all the created objects as spies.
 */
public class TestSpyWrapperStateBackend extends AbstractStateBackend implements CheckpointStorage {

    private final MemoryStateBackend delegate;

    public TestSpyWrapperStateBackend(MemoryStateBackend delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            KeyedStateBackendParameters<K> parameters) throws IOException {
        return spy(delegate.createKeyedStateBackend(parameters));
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            OperatorStateBackendParameters parameters) throws Exception {
        return spy(delegate.createOperatorStateBackend(parameters));
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
            throws IOException {
        return spy(delegate.resolveCheckpoint(externalPointer));
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
        return spy(delegate.createCheckpointStorage(jobId));
    }
}
