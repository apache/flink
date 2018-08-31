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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory.MemoryCheckpointOutputStream;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of a checkpoint storage for the {@link MemoryStateBackend}.
 * Depending on whether this is created with a checkpoint location, the setup supports
 * durable checkpoints (durable metadata) or not.
 */
public class MemoryBackendCheckpointStorage extends AbstractFsCheckpointStorage {

	/** The target directory for checkpoints (here metadata files only). Null, if not configured. */
	@Nullable
	private final Path checkpointsDirectory;

	/** The file system to persist the checkpoints to. Null if this does not durably persist checkpoints. */
	@Nullable
	private final FileSystem fileSystem;

	/** The maximum size of state stored in a state handle. */
	private final int maxStateSize;

	/**
	 * Creates a new MemoryBackendCheckpointStorage.
	 *
	 * @param jobId The ID of the job writing the checkpoints.
	 * @param checkpointsBaseDirectory The directory to write checkpoints to. May be null,
	 *                                 in which case this storage does not support durable persistence.
	 * @param defaultSavepointLocation The default savepoint directory, or null, if none is set.
	 * @param maxStateSize The maximum size of each individual piece of state.
	 *
	 * @throws IOException Thrown if a checkpoint base directory is given configured and the
	 *                     checkpoint directory cannot be created within that directory.
	 */
	public MemoryBackendCheckpointStorage(
			JobID jobId,
			@Nullable Path checkpointsBaseDirectory,
			@Nullable Path defaultSavepointLocation,
			int maxStateSize) throws IOException {

		super(jobId, defaultSavepointLocation);

		checkArgument(maxStateSize > 0);
		this.maxStateSize = maxStateSize;

		if (checkpointsBaseDirectory == null) {
			checkpointsDirectory = null;
			fileSystem = null;
		}
		else {
			this.fileSystem = checkpointsBaseDirectory.getFileSystem();
			this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointsBaseDirectory, jobId);

			fileSystem.mkdirs(checkpointsDirectory);
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the size (in bytes) that a individual chunk of state may have at most.
	 */
	public int getMaxStateSize() {
		return maxStateSize;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint Storage
	// ------------------------------------------------------------------------

	@Override
	public boolean supportsHighlyAvailableStorage() {
		return checkpointsDirectory != null;
	}

	@Override
	public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException {
		checkArgument(checkpointId >= 0);

		if (checkpointsDirectory != null) {
			// configured for durable metadata
			// prepare all the paths needed for the checkpoints
			checkState(fileSystem != null);

			final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

			// create the checkpoint exclusive directory
			fileSystem.mkdirs(checkpointDir);

			return new PersistentMetadataCheckpointStorageLocation(fileSystem, checkpointDir, maxStateSize);
		}
		else {
			// no durable metadata - typical in IDE or test setup case
			return new NonPersistentMetadataCheckpointStorageLocation(maxStateSize);
		}
	}

	@Override
	public CheckpointStreamFactory resolveCheckpointStorageLocation(
			long checkpointId,
			CheckpointStorageLocationReference reference) throws IOException {

		// no matter where the checkpoint goes, we always return the storage location that stores
		// state inline with the state handles.
		return new MemCheckpointStreamFactory(maxStateSize);
	}

	@Override
	public CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
		return new MemoryCheckpointOutputStream(maxStateSize);
	}

	@Override
	protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) throws IOException {
		return new PersistentMetadataCheckpointStorageLocation(fs, location, maxStateSize);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "MemoryBackendCheckpointStorage {" +
				"checkpointsDirectory=" + checkpointsDirectory +
				", fileSystem=" + fileSystem +
				", maxStateSize=" + maxStateSize +
				'}';
	}
}
