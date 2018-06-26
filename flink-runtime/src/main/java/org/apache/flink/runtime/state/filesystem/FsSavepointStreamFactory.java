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

import java.io.IOException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;

/**
 * A {@link CheckpointStreamFactory} that produces streams that write to a
 * {@link FileSystem}.
 *
 * <p>The difference to the parent {@link FsCheckpointStreamFactory} is only
 * in the created directory layout. All checkpoint files go to the checkpoint
 * directory.
 */
public class FsSavepointStreamFactory extends FsCheckpointStreamFactory {

	public FsSavepointStreamFactory(
			Path checkpointDataUri,
			JobID jobId,
			int fileStateSizeThreshold) throws IOException {

		super(checkpointDataUri, jobId, fileStateSizeThreshold,"");
	}

	@Override
	protected Path createBasePath(FileSystem fs, Path checkpointDirectory, JobID jobID,
								  String entropyInjectionKey) throws IOException {
		// No checkpoint specific directory required as the savepoint directory
		// is already unique.
		return checkpointDirectory;
	}

	@Override
	protected Path createCheckpointDirPath(Path checkpointDirectory, long checkpointID) {
		// No checkpoint specific directory required as the savepoint directory
		// is already unique.
		return checkpointDirectory;
	}
}
