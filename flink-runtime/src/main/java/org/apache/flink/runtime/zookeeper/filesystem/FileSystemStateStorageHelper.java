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

package org.apache.flink.runtime.zookeeper.filesystem;

import com.google.common.base.Preconditions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.filesystem.FileSerializableStateHandle;
import org.apache.flink.runtime.util.FileUtils;
import org.apache.flink.runtime.zookeeper.StateStorageHelper;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * {@link StateStorageHelper} implementation which stores the state in the given filesystem path.
 *
 * @param <T>
 */
public class FileSystemStateStorageHelper<T extends Serializable> implements StateStorageHelper<T> {

	private final Path rootPath;

	private final String prefix;

	private final FileSystem fs;

	public FileSystemStateStorageHelper(String rootPath, String prefix) throws IOException {
		this(new Path(rootPath), prefix);
	}

	public FileSystemStateStorageHelper(Path rootPath, String prefix) throws IOException {
		this.rootPath = Preconditions.checkNotNull(rootPath, "Root path");
		this.prefix = Preconditions.checkNotNull(prefix, "Prefix");

		fs = FileSystem.get(rootPath.toUri());
	}

	@Override
	public StateHandle<T> store(T state) throws Exception {
		Exception latestException = null;

		for (int attempt = 0; attempt < 10; attempt++) {
			Path filePath = getNewFilePath();
			FSDataOutputStream outStream;
			try {
				outStream = fs.create(filePath, false);
			}
			catch (Exception e) {
				latestException = e;
				continue;
			}

			try(ObjectOutputStream os = new ObjectOutputStream(outStream)) {
				os.writeObject(state);
			}

			return new FileSerializableStateHandle<>(filePath);
		}

		throw new Exception("Could not open output stream for state backend", latestException);
	}

	private Path getNewFilePath() {
		return new Path(rootPath, FileUtils.getRandomFilename(prefix));
	}
}
