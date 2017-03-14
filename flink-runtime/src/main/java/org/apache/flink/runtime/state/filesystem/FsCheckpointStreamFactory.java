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
import org.apache.flink.runtime.state.CheckpointStreamFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code CheckpointStreamFactory} that creates streams to persist checkpointed data to files.
 *
 * <p>The factory is configured with a base directory and persists the checkpoint data of specific
 * checkpoints in specific subdirectories. For example, if the base directory was set to 
 * {@code hdfs://namenode:port/flink-checkpoints/myJob-xyz/}, the stream factory would store all data
 * for checkpoint <i>17</i> under the path {@code hdfs://namenode:port/flink-checkpoints/myJob-xyz/chk-17/}.
 */
public class FsCheckpointStreamFactory implements CheckpointStreamFactory {

	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStreamFactory.class);

	/** Default size for the write buffer */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4096;

	/** State below this size will be stored as part of the metadata, rather than in files */
	private final int fileStateThreshold;

	/** The directory inside which this factory's streams store their files */
	private final Path basePath;

	/** Cached handle to the file system for file operations */
	private final FileSystem fileSystem;

	/**
	 * Creates a stream factory whose streams write to checkpoint subdirectories in the given base
	 * directory.
	 *
	 * @param filesystem
	 *            The FileSystem handle for the file system to write to. 
	 * @param basePath
	 *           The base bath of the directory in which the checkpoint specific subdirectories will be created.
	 * @param fileStateSizeThreshold
	 *           State up to this size will be stored as part of the metadata, rather than in files
	 */
	public FsCheckpointStreamFactory(FileSystem filesystem, Path basePath, int fileStateSizeThreshold) {
		checkArgument(fileStateSizeThreshold >= 0, "fileStateSizeThreshold must be >= 0");

		this.fileSystem = checkNotNull(filesystem);
		this.basePath = checkNotNull(basePath);
		this.fileStateThreshold = fileStateSizeThreshold;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Initialed file stream factory to URI {}.", basePath);
		}
	}

	@Override
	public FsCheckpointStateOutputStream createCheckpointStateOutputStream(
			long checkpointID, long timestamp) throws IOException {

		// only compute the path here and do NOT ensure that the directory exists!
		// That is because small and fast checkpoints frequently never write a file (all  data is stored
		// with metadata). In that case, we must not cause file system load with 'mkdirs()' commands
		Path checkpointDir = buildCheckpointPath(basePath, checkpointID);

		int bufferSize = Math.max(DEFAULT_WRITE_BUFFER_SIZE, fileStateThreshold);

		return new FsCheckpointStateOutputStream(checkpointDir, fileSystem, bufferSize, fileStateThreshold);
	}

	/**
	 * Computes the path in which the data of a checkpoint / savepoint with a specific checkpoint ID
	 * is stored.
	 * 
	 * <p>This method can be overridden by subclasses that want different directory structures.
	 * 
	 * @param basePath     The configured base directory.
	 * @param checkpointID The checkpoint ID.
	 * 
	 * @return The path where a specific checkpoint should store its state.
	 */
	protected Path buildCheckpointPath(Path basePath, long checkpointID) {
		return AbstractFileStateBackend.createSpecificCheckpointPath(basePath, checkpointID);
	}

	@Override
	public String toString() {
		return "File Stream Factory @ " + basePath;
	}
}
