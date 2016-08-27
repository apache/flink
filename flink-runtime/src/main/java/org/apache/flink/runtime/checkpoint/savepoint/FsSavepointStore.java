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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file system based {@link SavepointStore}.
 *
 * <p>Stored savepoints have the following format:
 * <pre>
 * MagicNumber SavepointVersion Savepoint
 *   - MagicNumber => int
 *   - SavepointVersion => int (returned by Savepoint#getVersion())
 *   - Savepoint => bytes (serialized via version-specific SavepointSerializer)
 * </pre>
 */
public class FsSavepointStore implements SavepointStore {

	private static final Logger LOG = LoggerFactory.getLogger(FsSavepointStore.class);

	/** Magic number for sanity checks against stored savepoints. */
	int MAGIC_NUMBER = 0x4960672d;

	/** Root path for savepoints. */
	private final Path rootPath;

	/** Prefix for savepoint files. */
	private final String prefix;

	/** File system to use for file access. */
	private final FileSystem fileSystem;

	/**
	 * Creates a new file system based {@link SavepointStore}.
	 *
	 * @param rootPath Root path for savepoints
	 * @param prefix   Prefix for savepoint files
	 * @throws IOException On failure to access root path
	 */
	FsSavepointStore(String rootPath, String prefix) throws IOException {
		this.rootPath = new Path(checkNotNull(rootPath, "Root path"));
		this.prefix = checkNotNull(prefix, "Prefix");

		this.fileSystem = FileSystem.get(this.rootPath.toUri());
	}

	@Override
	public <T extends Savepoint> String storeSavepoint(T savepoint) throws IOException {
		Preconditions.checkNotNull(savepoint, "Savepoint");

		Exception latestException = null;
		Path path = null;
		FSDataOutputStream fdos = null;

		// Try to create a FS output stream
		for (int attempt = 0; attempt < 10; attempt++) {
			path = new Path(rootPath, FileUtils.getRandomFilename(prefix));
			try {
				fdos = fileSystem.create(path, false);
				break;
			} catch (Exception e) {
				latestException = e;
			}
		}

		if (fdos == null) {
			throw new IOException("Failed to create file output stream at " + path, latestException);
		}

		boolean success = false;
		try (DataOutputStream dos = new DataOutputStream(fdos)) {
			// Write header
			dos.writeInt(MAGIC_NUMBER);
			dos.writeInt(savepoint.getVersion());

			// Write savepoint
			SavepointSerializer<T> serializer = SavepointSerializers.getSerializer(savepoint);
			serializer.serialize(savepoint, dos);
			success = true;
		} finally {
			if (!success && fileSystem.exists(path)) {
				if (!fileSystem.delete(path, true)) {
					LOG.warn("Failed to delete file " + path + " after failed write.");
				}
			}
		}

		return path.toString();
	}

	@Override
	public Savepoint loadSavepoint(String path) throws IOException {
		Preconditions.checkNotNull(path, "Path");

		try (DataInputStream dis = new DataInputViewStreamWrapper(createFsInputStream(new Path(path)))) {
			int magicNumber = dis.readInt();

			if (magicNumber == MAGIC_NUMBER) {
				int version = dis.readInt();

				SavepointSerializer<?> serializer = SavepointSerializers.getSerializer(version);
				return serializer.deserialize(dis);
			} else {
				throw new RuntimeException("Unexpected magic number. This is most likely " +
						"caused by trying to load a Flink 1.0 savepoint. You cannot load a " +
						"savepoint triggered by Flink 1.0 with this version of Flink. If it is " +
						"_not_ a Flink 1.0 savepoint, this error indicates that the specified " +
						"file is not a proper savepoint or the file has been corrupted.");
			}
		}
	}

	@Override
	public void disposeSavepoint(String path, ClassLoader classLoader) throws Exception {
		Preconditions.checkNotNull(path, "Path");
		Preconditions.checkNotNull(classLoader, "Class loader");

		try {
			Savepoint savepoint = loadSavepoint(path);
			savepoint.dispose(classLoader);

			Path filePath = new Path(path);

			if (fileSystem.exists(filePath)) {
				if (!fileSystem.delete(filePath, true)) {
					throw new IOException("Failed to delete " + filePath + ".");
				}
			} else {
				throw new IllegalArgumentException("Invalid path '" + filePath.toUri() + "'.");
			}
		} catch (Throwable t) {
			throw new IOException("Failed to dispose savepoint " + path + ".", t);
		}
	}

	@Override
	public void shutdown() throws Exception {
		// Nothing to do, because the savepoint life-cycle is independent of
		// the cluster life-cycle.
	}

	private FSDataInputStream createFsInputStream(Path path) throws IOException {
		if (fileSystem.exists(path)) {
			return fileSystem.open(path);
		} else {
			throw new IllegalArgumentException("Invalid path '" + path.toUri() + "'.");
		}
	}

	/**
	 * Returns the savepoint root path.
	 *
	 * @return Savepoint root path
	 */
	Path getRootPath() {
		return rootPath;
	}

}
