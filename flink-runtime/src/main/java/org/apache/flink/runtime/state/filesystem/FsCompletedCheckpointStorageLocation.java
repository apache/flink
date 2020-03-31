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
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;

import java.io.IOException;

/**
 * A {@link CompletedCheckpointStorageLocation} that resides on a file system.
 * This locattion is internally represented through the checkpoint directory plus the metadata file.
 */
public class FsCompletedCheckpointStorageLocation implements CompletedCheckpointStorageLocation {

	private static final long serialVersionUID = 1L;

	private final Path exclusiveCheckpointDir;

	private final FileStateHandle metadataFileHandle;

	private final String externalPointer;

	private transient FileSystem fs;

	public FsCompletedCheckpointStorageLocation(
			FileSystem fs,
			Path exclusiveCheckpointDir,
			FileStateHandle metadataFileHandle,
			String externalPointer) {

		this.fs = fs;
		this.exclusiveCheckpointDir = exclusiveCheckpointDir;
		this.metadataFileHandle = metadataFileHandle;
		this.externalPointer = externalPointer;
	}

	@Override
	public String getExternalPointer() {
		return externalPointer;
	}

	@Override
	public FileStateHandle getMetadataHandle() {
		return metadataFileHandle;
	}

	@Override
	public void disposeStorageLocation() throws IOException {
		if (fs == null) {
			fs = exclusiveCheckpointDir.getFileSystem();
		}
		fs.delete(exclusiveCheckpointDir, false);
	}
}
