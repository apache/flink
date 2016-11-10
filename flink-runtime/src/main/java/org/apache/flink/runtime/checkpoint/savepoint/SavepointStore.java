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
 * A file system based savepoint store.
 *
 * <p>Stored savepoints have the following format:
 * <pre>
 * MagicNumber SavepointVersion Savepoint
 *   - MagicNumber => int
 *   - SavepointVersion => int (returned by Savepoint#getVersion())
 *   - Savepoint => bytes (serialized via version-specific SavepointSerializer)
 * </pre>
 */
public class SavepointStore {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointStore.class);

	/** Magic number for sanity checks against stored savepoints. */
	public static final int MAGIC_NUMBER = 0x4960672d;

	/** Prefix for savepoint files. */
	private static final String prefix = "savepoint-";

	/**
	 * Stores the savepoint.
	 *
	 * @param targetDirectory Target directory to store savepoint in
	 * @param savepoint Savepoint to be stored
	 * @param <T>       Savepoint type
	 * @return Path of stored savepoint
	 * @throws Exception Failures during store are forwarded
	 */
	public static <T extends Savepoint> String storeSavepoint(
			String targetDirectory,
			T savepoint) throws IOException {

		checkNotNull(targetDirectory, "Target directory");
		checkNotNull(savepoint, "Savepoint");

		Exception latestException = null;
		Path path = null;
		FSDataOutputStream fdos = null;

		FileSystem fs = null;

		// Try to create a FS output stream
		for (int attempt = 0; attempt < 10; attempt++) {
			path = new Path(targetDirectory, FileUtils.getRandomFilename(prefix));

			if (fs == null) {
				fs = FileSystem.get(path.toUri());
			}

			try {
				fdos = fs.create(path, false);
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
			if (!success && fs.exists(path)) {
				if (!fs.delete(path, true)) {
					LOG.warn("Failed to delete file {} after failed write.", path);
				}
			}
		}

		return path.toString();
	}

	/**
	 * Loads the savepoint at the specified path.
	 *
	 * @param path Path of savepoint to load
	 * @return The loaded savepoint
	 * @throws Exception Failures during load are forwared
	 */
	public static Savepoint loadSavepoint(String path, ClassLoader userClassLoader) throws IOException {
		Preconditions.checkNotNull(path, "Path");

		try (DataInputStream dis = new DataInputViewStreamWrapper(createFsInputStream(new Path(path)))) {
			int magicNumber = dis.readInt();

			if (magicNumber == MAGIC_NUMBER) {
				int version = dis.readInt();

				SavepointSerializer<?> serializer = SavepointSerializers.getSerializer(version);
				return serializer.deserialize(dis, userClassLoader);
			} else {
				throw new RuntimeException("Unexpected magic number. This is most likely " +
						"caused by trying to load a Flink 1.0 savepoint. You cannot load a " +
						"savepoint triggered by Flink 1.0 with this version of Flink. If it is " +
						"_not_ a Flink 1.0 savepoint, this error indicates that the specified " +
						"file is not a proper savepoint or the file has been corrupted.");
			}
		}
	}

	/**
	 * Removes the savepoint meta data w/o loading and disposing it.
	 *
	 * @param path Path of savepoint to remove
	 * @throws Exception Failures during disposal are forwarded
	 */
	public static void removeSavepoint(String path) throws IOException {
		Preconditions.checkNotNull(path, "Path");

		try {
			LOG.info("Removing savepoint: {}.", path);

			Path filePath = new Path(path);
			FileSystem fs = FileSystem.get(filePath.toUri());

			if (fs.exists(filePath)) {
				if (!fs.delete(filePath, true)) {
					throw new IOException("Failed to delete " + filePath + ".");
				}
			} else {
				throw new IllegalArgumentException("Invalid path '" + filePath.toUri() + "'.");
			}
		} catch (Throwable t) {
			throw new IOException("Failed to dispose savepoint " + path + ".", t);
		}
	}

	private static FSDataInputStream createFsInputStream(Path path) throws IOException {
		FileSystem fs = FileSystem.get(path.toUri());

		if (fs.exists(path)) {
			return fs.open(path);
		} else {
			throw new IllegalArgumentException("Invalid path '" + path.toUri() + "'.");
		}
	}

}
