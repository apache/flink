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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A storage location for checkpoints on a file system.
 */
public class FsCheckpointStorageLocation implements CheckpointStorageLocation {

	private static final byte[] SUCCESS_MAGIC_BYTES = bytesFromLong(FsCheckpointStorage.SUCCESS_FILE_MAGIC_NUMBER);

	private final FileSystem fileSystem;

	private final Path checkpointDirectory;

	private final Path sharedStateDirectory;

	private final Path taskOwnedStateDirectory;

	private final Path metadataFilePath;

	private final Path successFilePath;

	private final String qualifiedCheckpointDirectory;

	public FsCheckpointStorageLocation(
			FileSystem fileSystem,
			Path checkpointDir,
			Path sharedStateDir,
			Path taskOwnedStateDir) {

		this.fileSystem = checkNotNull(fileSystem);
		this.checkpointDirectory = checkNotNull(checkpointDir);
		this.sharedStateDirectory = checkNotNull(sharedStateDir);
		this.taskOwnedStateDirectory = checkNotNull(taskOwnedStateDir);

		this.metadataFilePath = new Path(checkpointDir, AbstractFsCheckpointStorage.METADATA_FILE_NAME);
		this.successFilePath = new Path(checkpointDir, AbstractFsCheckpointStorage.SUCCESS_FILE_NAME);

		this.qualifiedCheckpointDirectory = checkpointDir.makeQualified(fileSystem).toString();
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public Path getCheckpointDirectory() {
		return checkpointDirectory;
	}

	public Path getSharedStateDirectory() {
		return sharedStateDirectory;
	}

	public Path getTaskOwnedStateDirectory() {
		return taskOwnedStateDirectory;
	}

	public Path getMetadataFilePath() {
		return metadataFilePath;
	}

	// ------------------------------------------------------------------------
	//  checkpoint metadata
	// ------------------------------------------------------------------------

	@Override
	public CheckpointStateOutputStream createMetadataOutputStream() throws IOException {
		return new FixFileFsStateOutputStream(fileSystem, metadataFilePath);
	}

	@Override
	public String markCheckpointAsFinished() throws IOException {
		// we later want to write a success file, but currently cannot do that due to missing
		// directory-style cleanup logic (which we in turn cannot do because incremental
		// checkpoints leave chunks in the directories that must not be deleted)
//		try (FSDataOutputStream out = fileSystem.create(successFilePath, WriteMode.NO_OVERWRITE)) {
//			out.write(SUCCESS_MAGIC_BYTES);
//		}

		return qualifiedCheckpointDirectory;
	}

	@Override
	public void disposeOnFailure() throws IOException {
		// on a failure, no chunk in the checkpoint directory needs to be saved, so
		// we can drop it as a whole
		fileSystem.delete(checkpointDirectory, true);
	}

	@Override
	public String getLocationAsPointer() {
		return qualifiedCheckpointDirectory;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "FsCheckpointStorageLocation {" +
				"metadataFilePath=" + metadataFilePath +
				", taskOwnedStateDirectory=" + taskOwnedStateDirectory +
				", sharedStateDirectory=" + sharedStateDirectory +
				", checkpointDirectory=" + checkpointDirectory +
				'}';
	}

	private static byte[] bytesFromLong(long value) {
		final byte[] bytes = new byte[8];
		for (int i = 0; i < 8; i++) {
			bytes[i] = (byte) value;
			value >>>= 8;
		}
		return bytes;
	}
}
