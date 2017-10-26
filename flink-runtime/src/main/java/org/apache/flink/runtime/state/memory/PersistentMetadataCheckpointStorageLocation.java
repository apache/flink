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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;

/**
 * A checkpoint storage location for the {@link MemoryStateBackend} when it durably
 * persists the metadata in a file system.
 *
 * <p>This class inherits its behavior for metadata from the {@link FsCheckpointStorageLocation},
 * which makes checkpoint metadata cross compatible between the two classes and hence between
 * the {@link org.apache.flink.runtime.state.filesystem.FsStateBackend FsStateBackend} and the
 * {@link MemoryStateBackend}.
 */
public class PersistentMetadataCheckpointStorageLocation extends FsCheckpointStorageLocation {

	/** The internal pointer for the {@link MemoryStateBackend}'s storage location (data inline with
	 * state handles) that gets sent to the TaskManagers to describe this storage. */
	static final String LOCATION_POINTER = "(embedded)";

	/**
	 * Creates a checkpoint storage persists metadata to a file system and stores state
	 * in line in state handles with the metadata.
	 *
	 * @param fileSystem The file system to which the metadata will be written.
	 * @param checkpointDir The directory where the checkpoint metadata will be written.
	 */
	public PersistentMetadataCheckpointStorageLocation(FileSystem fileSystem, Path checkpointDir) {
		super(fileSystem, checkpointDir, checkpointDir, checkpointDir);
	}

	// ------------------------------------------------------------------------

	@Override
	public String getLocationAsPointer() {
		return LOCATION_POINTER;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getClass().getName() + " @ " + getCheckpointDirectory();
	}
}
