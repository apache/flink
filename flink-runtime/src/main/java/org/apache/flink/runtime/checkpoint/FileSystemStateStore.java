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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileSerializableStateHandle;
import org.apache.flink.runtime.util.FileUtils;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link FileSystem} backed {@link StateStore}.
 *
 * @param <T> Type of state
 */
class FileSystemStateStore<T extends Serializable> implements StateStore<T> {

	private final Path rootPath;

	private final String prefix;

	private final FileSystem fileSystem;

	FileSystemStateStore(String rootPath, String prefix) throws IOException {
		this(new Path(rootPath), prefix);
	}

	FileSystemStateStore(Path rootPath, String prefix) throws IOException {
		this.rootPath = checkNotNull(rootPath, "Root path");
		this.prefix = checkNotNull(prefix, "Prefix");

		this.fileSystem = FileSystem.get(rootPath.toUri());
	}

	@Override
	public String putState(T state) throws Exception {
		Exception latestException = null;

		for (int attempt = 0; attempt < 10; attempt++) {
			Path filePath = new Path(rootPath, FileUtils.getRandomFilename(prefix));
			FSDataOutputStream outStream;
			try {
				outStream = fileSystem.create(filePath, false);
			}
			catch (Exception e) {
				latestException = e;
				continue;
			}

			try (ObjectOutputStream os = new ObjectOutputStream(outStream)) {
				os.writeObject(state);
			}

			return filePath.toString();
		}

		throw new IOException("Failed to open file output stream", latestException);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T getState(String path) throws Exception {
		Path filePath = new Path(path);

		if (FileSystem.get(filePath.toUri()).exists(filePath)) {
			return (T) new FileSerializableStateHandle<>(filePath)
					.getState(ClassLoader.getSystemClassLoader());
		}
		else {
			throw new IllegalArgumentException("Invalid path '" + filePath.toUri() + "'.");
		}
	}

	@Override
	public void disposeState(String path) throws Exception {
		Path filePath = new Path(path);

		if (!fileSystem.delete(filePath, true)) {
			throw new IllegalArgumentException("Invalid path '" + filePath.toUri() + "'.");
		}

		// send a call to delete the directory containing the file. this will
		// fail (and be ignored) when some files still exist
		try {
			fileSystem.delete(filePath.getParent(), false);
		}
		catch (IOException ignored) {
		}
	}

	Path getRootPath() {
		return rootPath;
	}
}
