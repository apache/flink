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
package org.apache.flink.runtime.state.filemerging;

import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.Collections;
import java.util.Map;

/**
 * An empty {@link FileMergingOperatorStreamStateHandle} that is only used as a placeholder to
 * prevent file merging directory from being deleted.
 */
public class EmptyFileMergingOperatorStreamStateHandle
        extends FileMergingOperatorStreamStateHandle {

    private static final long serialVersionUID = 1L;

    public EmptyFileMergingOperatorStreamStateHandle(
            DirectoryStreamStateHandle taskOwnedDirHandle,
            DirectoryStreamStateHandle sharedDirHandle,
            Map<String, StateMetaInfo> stateNameToPartitionOffsets,
            StreamStateHandle delegateStateHandle) {
        super(
                taskOwnedDirHandle,
                sharedDirHandle,
                stateNameToPartitionOffsets,
                delegateStateHandle);
    }

    /**
     * Create an empty {@link EmptyFileMergingOperatorStreamStateHandle}.
     *
     * @param taskownedDirHandle the directory where operator state is stored.
     * @param sharedDirHandle the directory where shared state is stored.
     */
    public static EmptyFileMergingOperatorStreamStateHandle create(
            DirectoryStreamStateHandle taskownedDirHandle,
            DirectoryStreamStateHandle sharedDirHandle) {
        final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
                Collections.emptyMap();
        return new EmptyFileMergingOperatorStreamStateHandle(
                taskownedDirHandle,
                sharedDirHandle,
                writtenStatesMetaData,
                EmptySegmentFileStateHandle.INSTANCE);
    }
}
