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

package org.apache.flink.migration.v0.runtime.filesystem;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.v0.runtime.AbstractCloseableHandleV0;
import org.apache.flink.migration.v0.runtime.StateObjectV0;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A handle to the files storing states in SavepointV0.
 */
@Deprecated
@SuppressWarnings("deprecation")
public abstract class AbstractFileStateHandleV0 extends AbstractCloseableHandleV0 implements StateObjectV0 {

	private static final long serialVersionUID = 350284443258002355L;

	/** The path to the file in the filesystem, fully describing the file system */
	private final Path filePath;

	/** Cached file system handle */
	private transient FileSystem fs;

	/**
	 * Creates a new file state for the given file path.
	 * 
	 * @param filePath The path to the file that stores the state.
	 */
	protected AbstractFileStateHandleV0(Path filePath) {
		this.filePath = checkNotNull(filePath);
	}

	/**
	 * Gets the path where this handle's state is stored.
	 * @return The path where this handle's state is stored.
	 */
	public Path getFilePath() {
		return filePath;
	}

	/**
	 * Gets the file system that stores the file state.
	 * @return The file system that stores the file state.
	 * @throws IOException Thrown if the file system cannot be accessed.
	 */
	protected FileSystem getFileSystem() throws IOException {
		if (fs == null) {
			fs = FileSystem.get(filePath.toUri());
		}
		return fs;
	}

	/**
	 * Returns the file size in bytes.
	 *
	 * @return The file size in bytes.
	 * @throws IOException Thrown if the file system cannot be accessed.
	 */
	public long getFileSize() throws IOException {
		return getFileSystem().getFileStatus(filePath).getLen();
	}
}
