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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.util.FileUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of durable checkpoint storage to file systems.
 */
public abstract class AbstractFsCheckpointStorage implements CheckpointStorage {

	// ------------------------------------------------------------------------
	//  Constants
	// ------------------------------------------------------------------------

	/** The prefix of the directory containing the data exclusive to a checkpoint. */
	public static final String CHECKPOINT_DIR_PREFIX = "chk-";

	/** The name of the directory for shared checkpoint state. */
	public static final String CHECKPOINT_SHARED_STATE_DIR = "shared";

	/** The name of the directory for state not owned/released by the master, but by the TaskManagers. */
	public static final String CHECKPOINT_TASK_OWNED_STATE_DIR = "taskowned";

	/** The name of the metadata files in checkpoints / savepoints. */
	public static final String METADATA_FILE_NAME = "_metadata";

	/** The magic number that is put in front of any reference. */
	private static final byte[] REFERENCE_MAGIC_NUMBER = new byte[] { 0x05, 0x5F, 0x3F, 0x18 };

	// ------------------------------------------------------------------------
	//  Fields and properties
	// ------------------------------------------------------------------------

	/** The jobId, written into the generated savepoint directories. */
	private final JobID jobId;

	/** The default location for savepoints. Null, if none is configured. */
	@Nullable
	private final Path defaultSavepointDirectory;

	/**
	 * Creates a new checkpoint storage.
	 *
	 * @param jobId The ID of the job that writes the checkpoints.
	 * @param defaultSavepointDirectory The default location for savepoints, or null, if none is set.
	 */
	protected AbstractFsCheckpointStorage(
			JobID jobId,
			@Nullable Path defaultSavepointDirectory) {

		this.jobId = checkNotNull(jobId);
		this.defaultSavepointDirectory = defaultSavepointDirectory;
	}

	/**
	 * Gets the default directory for savepoints. Returns null, if no default savepoint
	 * directory is configured.
	 */
	@Nullable
	public Path getDefaultSavepointDirectory() {
		return defaultSavepointDirectory;
	}

	// ------------------------------------------------------------------------
	//  CheckpointStorage implementation
	// ------------------------------------------------------------------------

	@Override
	public boolean hasDefaultSavepointLocation() {
		return defaultSavepointDirectory != null;
	}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String checkpointPointer) throws IOException {
		return resolveCheckpointPointer(checkpointPointer);
	}

	/**
	 * Creates a file system based storage location for a savepoint.
	 *
	 * <p>This methods implements the logic that decides which location to use (given optional
	 * parameters for a configured location and a location passed for this specific savepoint)
	 * and how to name and initialize the savepoint directory.
	 *
	 * @param externalLocationPointer    The target location pointer for the savepoint.
	 *                                   Must be a valid URI. Null, if not supplied.
	 * @param checkpointId               The checkpoint ID of the savepoint.
	 *
	 * @return The checkpoint storage location for the savepoint.
	 *
	 * @throws IOException Thrown if the target directory could not be created.
	 */
	@Override
	public CheckpointStorageLocation initializeLocationForSavepoint(
			@SuppressWarnings("unused") long checkpointId,
			@Nullable String externalLocationPointer) throws IOException {

		// determine where to write the savepoint to

		final Path savepointBasePath;
		if (externalLocationPointer != null) {
			savepointBasePath = new Path(externalLocationPointer);
		}
		else if (defaultSavepointDirectory != null) {
			savepointBasePath = defaultSavepointDirectory;
		}
		else {
			throw new IllegalArgumentException("No savepoint location given and no default location configured.");
		}

		// generate the savepoint directory

		final FileSystem fs = savepointBasePath.getFileSystem();
		final String prefix = "savepoint-" + jobId.toString().substring(0, 6) + '-';

		Exception latestException = null;
		for (int attempt = 0; attempt < 10; attempt++) {
			final Path path = new Path(savepointBasePath, FileUtils.getRandomFilename(prefix));

			try {
				if (fs.mkdirs(path)) {
					// we make the path qualified, to make it independent of default schemes and authorities
					final Path qp = path.makeQualified(fs);

					return createSavepointLocation(fs, qp);
				}
			} catch (Exception e) {
				latestException = e;
			}
		}

		throw new IOException("Failed to create savepoint directory at " + savepointBasePath, latestException);
	}

	protected abstract CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) throws IOException;

	// ------------------------------------------------------------------------
	//  Creating and resolving paths
	// ------------------------------------------------------------------------

	/**
	 * Builds directory into which a specific job checkpoints, meaning the directory inside which
	 * it creates the checkpoint-specific subdirectories.
	 *
	 * <p>This method only succeeds if a base checkpoint directory has been set; otherwise
	 * the method fails with an exception.
	 *
	 * @param jobId The ID of the job
	 * @return The job's checkpoint directory, re
	 *
	 * @throws UnsupportedOperationException Thrown, if no base checkpoint directory has been set.
	 */
	protected static Path getCheckpointDirectoryForJob(Path baseCheckpointPath, JobID jobId) {
		return new Path(baseCheckpointPath, jobId.toString());
	}

	/**
	 * Creates the directory path for the data exclusive to a specific checkpoint.
	 *
	 * @param baseDirectory The base directory into which the job checkpoints.
	 * @param checkpointId The ID (logical timestamp) of the checkpoint.
	 */
	protected static Path createCheckpointDirectory(Path baseDirectory, long checkpointId) {
		return new Path(baseDirectory, CHECKPOINT_DIR_PREFIX + checkpointId);
	}

	/**
	 * Takes the given string (representing a pointer to a checkpoint) and resolves it to a file
	 * status for the checkpoint's metadata file.
	 *
	 * @param checkpointPointer The pointer to resolve.
	 * @return A state handle to checkpoint/savepoint's metadata.
	 *
	 * @throws IOException Thrown, if the pointer cannot be resolved, the file system not accessed, or
	 *                     the pointer points to a location that does not seem to be a checkpoint/savepoint.
	 */
	@Internal
	public static CompletedCheckpointStorageLocation resolveCheckpointPointer(String checkpointPointer) throws IOException {
		checkNotNull(checkpointPointer, "checkpointPointer");
		checkArgument(!checkpointPointer.isEmpty(), "empty checkpoint pointer");

		// check if the pointer is in fact a valid file path
		final Path path;
		try {
			path = new Path(checkpointPointer);
		}
		catch (Exception e) {
			throw new IOException("Checkpoint/savepoint path '" + checkpointPointer + "' is not a valid file URI. " +
					"Either the pointer path is invalid, or the checkpoint was created by a different state backend.");
		}

		// check if the file system can be accessed
		final FileSystem fs;
		try {
			fs = path.getFileSystem();
		}
		catch (IOException e) {
			throw new IOException("Cannot access file system for checkpoint/savepoint path '" +
					checkpointPointer + "'.", e);
		}

		final FileStatus status;
		try {
			status = fs.getFileStatus(path);
		}
		catch (FileNotFoundException e) {
			throw new FileNotFoundException("Cannot find checkpoint or savepoint " +
					"file/directory '" + checkpointPointer + "' on file system '" + fs.getUri().getScheme() + "'.");
		}

		// if we are here, the file / directory exists
		final Path checkpointDir;
		final FileStatus metadataFileStatus;

		// If this is a directory, we need to find the meta data file
		if (status.isDir()) {
			checkpointDir = status.getPath();
			final Path metadataFilePath = new Path(path, METADATA_FILE_NAME);
			try {
				metadataFileStatus = fs.getFileStatus(metadataFilePath);
			}
			catch (FileNotFoundException e) {
				throw new FileNotFoundException("Cannot find meta data file '" + METADATA_FILE_NAME +
						"' in directory '" + path + "'. Please try to load the checkpoint/savepoint " +
						"directly from the metadata file instead of the directory.");
			}
		}
		else {
			// this points to a file and we either do no name validation, or
			// the name is actually correct, so we can return the path
			metadataFileStatus = status;
			checkpointDir = status.getPath().getParent();
		}

		final FileStateHandle metaDataFileHandle = new FileStateHandle(
				metadataFileStatus.getPath(), metadataFileStatus.getLen());

		final String pointer = checkpointDir.makeQualified(fs).toString();

		return new FsCompletedCheckpointStorageLocation(
				fs,
				checkpointDir,
				metaDataFileHandle,
				pointer);
	}

	// ------------------------------------------------------------------------
	//  Encoding / Decoding of References
	// ------------------------------------------------------------------------

	/**
	 * Encodes the given path as a reference in bytes. The path is encoded as a UTF-8 string
	 * and prepended as a magic number.
	 *
	 * @param path The path to encode.
	 * @return The location reference.
	 */
	public static CheckpointStorageLocationReference encodePathAsReference(Path path) {
		byte[] refBytes = path.toString().getBytes(StandardCharsets.UTF_8);
		byte[] bytes = new byte[REFERENCE_MAGIC_NUMBER.length + refBytes.length];

		System.arraycopy(REFERENCE_MAGIC_NUMBER, 0, bytes, 0, REFERENCE_MAGIC_NUMBER.length);
		System.arraycopy(refBytes, 0, bytes, REFERENCE_MAGIC_NUMBER.length, refBytes.length);

		return new CheckpointStorageLocationReference(bytes);
	}

	/**
	 * Decodes the given reference into a path. This method validates that the reference bytes start with
	 * the correct magic number (as written by {@link #encodePathAsReference(Path)}) and converts
	 * the remaining bytes back to a proper path.
	 *
	 * @param reference The bytes representing the reference.
	 * @return The path decoded from the reference.
	 *
	 * @throws IllegalArgumentException Thrown, if the bytes do not represent a proper reference.
	 */
	public static Path decodePathFromReference(CheckpointStorageLocationReference reference) {
		if (reference.isDefaultReference()) {
			throw new IllegalArgumentException("Cannot decode default reference");
		}

		final byte[] bytes = reference.getReferenceBytes();
		final int headerLen = REFERENCE_MAGIC_NUMBER.length;

		if (bytes.length > headerLen) {
			// compare magic number
			for (int i = 0; i < headerLen; i++) {
				if (bytes[i] != REFERENCE_MAGIC_NUMBER[i]) {
					throw new IllegalArgumentException("Reference starts with the wrong magic number");
				}
			}

			// covert to string and path
			try {
				return new Path(new String(bytes, headerLen, bytes.length - headerLen, StandardCharsets.UTF_8));
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Reference cannot be decoded to a path", e);
			}
		}
		else {
			throw new IllegalArgumentException("Reference too short.");
		}
	}
}
