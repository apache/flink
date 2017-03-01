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
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This stream factory aids in persisting checkpoint metadata to a configured location. 
 */
public class FsCheckpointMetadataStreamFactory implements CheckpointMetadataStreamFactory {

	/** The directory into which the checkpoint/savepoint is stored */
	private final Path checkpointDirectory;

	/** The path of the checkpoint / savepoint metadata file */
	private final Path checkpointMetadataFile;

	/** Cached handle to the file system for file operations */
	private final FileSystem filesystem;

	/**
	 * Creates a new metadata stream factory.
	 * 
	 * @param filesystem The handle to the file system for file operations
	 * @param checkpointDirectory The directory into which the checkpoint/savepoint is stored
	 * @param checkpointMetadataFile The path of the checkpoint / savepoint metadata file
	 */
	public FsCheckpointMetadataStreamFactory(FileSystem filesystem, Path checkpointDirectory, Path checkpointMetadataFile) {
		this.filesystem = checkNotNull(filesystem);
		this.checkpointDirectory = checkNotNull(checkpointDirectory);
		this.checkpointMetadataFile = checkNotNull(checkpointMetadataFile);
	}


	@Override
	public CheckpointMetadataOutputStream createCheckpointStateOutputStream() throws IOException {
		return new FsCheckpointMetadataOutputStream(filesystem, checkpointMetadataFile);
	}

	@Override
	public String getTargetLocation() {
		return checkpointDirectory.toString();
	}
}
