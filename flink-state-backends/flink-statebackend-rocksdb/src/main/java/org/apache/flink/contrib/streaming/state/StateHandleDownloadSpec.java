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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;

import java.nio.file.Path;

/**
 * This class represents a download specification for the content of one {@link
 * IncrementalRemoteKeyedStateHandle} to a target {@link Path}.
 */
public class StateHandleDownloadSpec {
    /** The state handle to download. */
    private final IncrementalRemoteKeyedStateHandle stateHandle;

    /** The path to which the content of the state handle shall be downloaded. */
    private final Path downloadDestination;

    public StateHandleDownloadSpec(
            IncrementalRemoteKeyedStateHandle stateHandle, Path downloadDestination) {
        this.stateHandle = stateHandle;
        this.downloadDestination = downloadDestination;
    }

    public IncrementalRemoteKeyedStateHandle getStateHandle() {
        return stateHandle;
    }

    public Path getDownloadDestination() {
        return downloadDestination;
    }

    public IncrementalLocalKeyedStateHandle createLocalStateHandleForDownloadedState() {
        return new IncrementalLocalKeyedStateHandle(
                stateHandle.getBackendIdentifier(),
                stateHandle.getCheckpointId(),
                new DirectoryStateHandle(downloadDestination),
                stateHandle.getKeyGroupRange(),
                stateHandle.getMetaDataStateHandle(),
                stateHandle.getSharedState());
    }
}
