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

import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A factory for checkpoint output streams, which are used to persist data for checkpoints.
 *
 * <p>Stream factories can be created from the {@link CheckpointStorageAccess} through {@link
 * CheckpointStorageAccess#resolveCheckpointStorageLocation(long,
 * CheckpointStorageLocationReference)}.
 */
public interface CheckpointStreamFactory {

    /**
     * Creates an new {@link CheckpointStateOutputStream}. When the stream is closed, it returns a
     * state handle that can retrieve the state back.
     *
     * @param scope The state's scope, whether it is exclusive or shared.
     * @return An output stream that writes state for the given checkpoint.
     * @throws IOException Exceptions may occur while creating the stream and should be forwarded.
     */
    CheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope)
            throws IOException;

    /**
     * Tells if we can duplicate the given {@link StreamStateHandle} into the path corresponding to
     * the given {@link CheckpointedStateScope}.
     *
     * <p>This should be a rather cheap operation, preferably not involving any remote accesses.
     *
     * @param stateHandle The handle to duplicate
     * @param scope Scope determining the location to duplicate into
     * @return true, if we can perform the duplication
     */
    boolean canFastDuplicate(StreamStateHandle stateHandle, CheckpointedStateScope scope)
            throws IOException;

    /**
     * Duplicates {@link StreamStateHandle} into the path corresponding to * the given {@link
     * CheckpointedStateScope}.
     *
     * <p>You should first check if you can duplicate with {@link
     * #canFastDuplicate(StreamStateHandle, CheckpointedStateScope)}.
     *
     * @param stateHandles The handles to duplicate
     * @param scope Scope determining the location to duplicate into
     * @return The duplicated handle
     */
    List<StreamStateHandle> duplicate(
            List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) throws IOException;

    /**
     * A callback method when some previous handle is reused. It is needed by the file merging
     * mechanism (FLIP-306) which will manage the life cycle of underlying files by file-reusing
     * information.
     *
     * @param previousHandle the previous handles that will be reused.
     */
    default void reusePreviousStateHandle(Collection<? extends StreamStateHandle> previousHandle) {
        // Does nothing for normal stream factory
    }

    /**
     * A pre-check hook before the checkpoint writer want to reuse a state handle, if this returns
     * false, it is not recommended for the writer to rewrite the state file considering the space
     * amplification.
     *
     * @param stateHandle the handle to be reused.
     * @return true if it can be reused.
     */
    default boolean couldReuseStateHandle(StreamStateHandle stateHandle) {
        // By default, the CheckpointStreamFactory doesn't support snapshot-file-merging, so the
        // SegmentFileStateHandle type of stateHandle can not be reused.
        return !FileMergingSnapshotManager.isFileMergingHandle(stateHandle);
    }
}
