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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of durable checkpoint storage to file systems supporting segments.
 */
public class FsSegmentCheckpointStorage extends AbstractFsCheckpointStorage {

	private static final Logger LOG = LoggerFactory.getLogger(FsSegmentCheckpointStorage.class);

	private final FileSystem fileSystem;

	private final Path checkpointsDirectory;

	private final Path sharedStateDirectory;

	private final Path taskOwnedStateDirectory;

	/** file size thread for FsCheckpointStorageLocation. */
	private final int fileSizeThreshold;

	/** write buffer size for checkpoint output stream. */
	private final int writeBufferSize;

	private volatile boolean directoriesInitialized = false;

	private final int maxConcurrentCheckpoints;

	public FsSegmentCheckpointStorage(
		Path checkpointBaseDirectory,
		@Nullable Path defaultSavepointDirectory,
		JobID jobId,
		int fileSizeThreshold,
		int writeBufferSize,
		int maxConcurrentCheckpoints) throws IOException {
		this(checkpointBaseDirectory.getFileSystem(), checkpointBaseDirectory, defaultSavepointDirectory, jobId, fileSizeThreshold, writeBufferSize, maxConcurrentCheckpoints);
	}

	public FsSegmentCheckpointStorage(
		FileSystem fs,
		Path checkpointBaseDirectory,
		@Nullable Path defaultSavepointDirectory,
		JobID jobId,
		int fileSizeThreshold,
		int writeBufferSize,
		int maxConcurrentCheckpoints) {
		super(jobId, defaultSavepointDirectory);

		checkNotNull(fs);
		checkNotNull(checkpointBaseDirectory);
		checkArgument(fileSizeThreshold >= 0);
		checkArgument(writeBufferSize >= 0);
		checkArgument(maxConcurrentCheckpoints > 0);

		this.fileSystem = fs;
		this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId);
		this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
		this.taskOwnedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
		this.fileSizeThreshold = fileSizeThreshold;
		this.writeBufferSize = writeBufferSize;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
	}

	@Override
	protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
		final CheckpointStorageLocationReference reference = encodePathAsReference(location);
		// for savepoint, we would not use sharing file mechanism.
		return new FsCheckpointStorageLocation(fs, location, location, location, reference, fileSizeThreshold, writeBufferSize);
	}

	@Override
	public boolean supportsHighlyAvailableStorage() {
		return true;
	}

	public Path getCheckpointsDirectory() {
		return checkpointsDirectory;
	}

	@Override
	public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException {
		checkArgument(checkpointId > 0);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Initializing location for checkpoint {} with sharedStateDirectory {}, taskOwnedStateDirectory {}",
				checkpointId, sharedStateDirectory, taskOwnedStateDirectory);
		}
		if (!directoriesInitialized) {
			fileSystem.mkdirs(checkpointsDirectory);
			fileSystem.mkdirs(sharedStateDirectory);
			fileSystem.mkdirs(taskOwnedStateDirectory);
			directoriesInitialized = true;
		}

		// prepare all the paths needed for the checkpoints
		final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

		// create the checkpoint exclusive directory
		fileSystem.mkdirs(checkpointDir);

		return new FsSegmentCheckpointStorageLocation(fileSystem,
			checkpointDir,
			sharedStateDirectory,
			taskOwnedStateDirectory,
			CheckpointStorageLocationReference.getDefault(),
			fileSizeThreshold,
			maxConcurrentCheckpoints);
	}

	@Override
	public CheckpointStreamFactory resolveCheckpointStorageLocation(
		long checkpointId,
		CheckpointStorageLocationReference reference) throws IOException {

		if (reference.isDefaultReference()) {
			// default reference, construct the default location for that particular checkpoint
			final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

			return new FsSegmentCheckpointStorageLocation(
					fileSystem,
					checkpointDir,
					sharedStateDirectory,
					taskOwnedStateDirectory,
					reference,
					fileSizeThreshold,
					maxConcurrentCheckpoints);
		}
		else {
			// location encoded in the reference, means savepoint,
			// savepoint uses FsCheckpointStorageLocation.
			final Path path = decodePathFromReference(reference);

			return new FsCheckpointStorageLocation(
				path.getFileSystem(),
				path,
				path,
				path,
				reference,
				fileSizeThreshold,
				writeBufferSize);
		}
	}

	@Override
	public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() {
		// task owned state use FsCheckpointStateOutputStream.
		return new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
			taskOwnedStateDirectory,
			fileSystem,
			writeBufferSize,
			fileSizeThreshold);
	}

	@Override
	public String toString() {
		return "FsSegmentCheckpointStorage{" +
			"fileSystem=" + fileSystem +
			", checkpointsDirectory=" + checkpointsDirectory +
			", sharedStateDirectory=" + sharedStateDirectory +
			", taskOwnedStateDirectory=" + taskOwnedStateDirectory +
			", fileSizeThreshold=" + fileSizeThreshold +
			", writeBufferSize=" + writeBufferSize +
			'}';
	}
}

