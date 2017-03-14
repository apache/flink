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

/**
 * A {@link CheckpointStreamFactory} for writing savepoint state.
 *
 * <p>This factory differs from the {@link FsCheckpointStreamFactory} in that it does not create
 * a checkpoint sub-directory, but directly writes to the given target directory.
 */
public class FsSavepointStreamFactory extends FsCheckpointStreamFactory {

	/**
	 * Creates a stream factory whose streams write given directory path.
	 *
	 * @param filesystem
	 *            The FileSystem handle for the file system to write to. 
	 * @param savepointDirectory
	 *           The directory in which the files created by the streams will be stored.
	 * @param fileStateSizeThreshold
	 *           State up to this size will be stored as part of the metadata, rather than in files
	 */
	public FsSavepointStreamFactory(FileSystem filesystem, Path savepointDirectory, int fileStateSizeThreshold) {
		super(filesystem, savepointDirectory, fileStateSizeThreshold);
	}

	/**
	 * Sets the directory for the state object to the exact configured savepoint directory.
	 */
	@Override
	protected Path buildCheckpointPath(Path checkpointPath, long checkpointID) {
		return checkpointPath;
	}
}
