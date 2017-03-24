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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities for storing and loading savepoint meta data files.
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

	private static final String SAVEPOINT_METADATA_FILE = "_metadata";

	/**
	 * Metadata file for an externalized checkpoint, random suffix added
	 * during store, because the parent directory is not unique.
	 */
	static final String EXTERNALIZED_CHECKPOINT_METADATA_FILE = "checkpoint_metadata-";

	/**
	 * Creates a savepoint directory.
	 *
	 * @param baseDirectory Base target directory for the savepoint
	 * @param jobId Optional JobID the savepoint belongs to
	 * @return The created savepoint directory
	 * @throws IOException FileSystem operation failures are forwarded
	 */
	public static String createSavepointDirectory(@Nonnull String baseDirectory, @Nullable JobID jobId) throws IOException {
		final Path basePath = new Path(baseDirectory);
		final FileSystem fs = basePath.getFileSystem();

		final String prefix;
		if (jobId == null) {
			prefix = "savepoint-";
		} else {
			prefix = String.format("savepoint-%s-", jobId.toString().substring(0, 6));
		}

		Exception latestException = null;

		// Try to create a FS output stream
		for (int attempt = 0; attempt < 10; attempt++) {
			Path path = new Path(basePath, FileUtils.getRandomFilename(prefix));

			try {
				if (fs.mkdirs(path)) {
					return path.toString();
				}
			} catch (Exception e) {
				latestException = e;
			}
		}

		throw new IOException("Failed to create savepoint directory at " + baseDirectory, latestException);
	}

	/**
	 * Deletes a savepoint directory.
	 *
	 * @param savepointDirectory Recursively deletes the given directory
	 * @throws IOException FileSystem operation failures are forwarded
	 */
	public static void deleteSavepointDirectory(@Nonnull String savepointDirectory) throws IOException {
		Path path = new Path(savepointDirectory);
		FileSystem fs = FileSystem.get(path.toUri());
		fs.delete(path, true);
	}

	/**
	 * Stores the savepoint metadata file.
	 *
	 * @param <T>       Savepoint type
	 * @param directory Target directory to store savepoint in
	 * @param savepoint Savepoint to be stored
	 * @return Path of stored savepoint
	 * @throws IOException Failures during store are forwarded
	 */
	public static <T extends Savepoint> String storeSavepoint(String directory, T savepoint) throws IOException {
		// write and create the file handle
		FileStateHandle metadataFileHandle = storeSavepointToHandle(directory,
			SAVEPOINT_METADATA_FILE, savepoint);

		// we return the savepoint directory path here!
		// The directory path also works to resume from and is more elegant than the direct
		// metadata file pointer
		return metadataFileHandle.getFilePath().getParent().toString();
	}

	/**
	 * Stores the savepoint metadata file to a state handle.
	 *
	 * @param directory Target directory to store savepoint in
	 * @param savepoint Savepoint to be stored
	 *
	 * @return State handle to the checkpoint metadata
	 * @throws IOException Failures during store are forwarded
	 */
	public static <T extends Savepoint> FileStateHandle storeSavepointToHandle(String directory, T savepoint) throws IOException {
		return storeSavepointToHandle(directory, SAVEPOINT_METADATA_FILE, savepoint);
	}

	/**
	 * Stores the externalized checkpoint metadata file to a state handle.
	 *
	 * @param directory Target directory to store savepoint in
	 * @param savepoint Savepoint to be stored
	 *
	 * @return State handle to the checkpoint metadata
	 * @throws IOException Failures during store are forwarded
	 */
	public static <T extends Savepoint> FileStateHandle storeExternalizedCheckpointToHandle(String directory, T savepoint) throws IOException {
		String fileName = FileUtils.getRandomFilename(EXTERNALIZED_CHECKPOINT_METADATA_FILE);
		return storeSavepointToHandle(directory, fileName, savepoint);
	}

	/**
	 * Stores the savepoint metadata file to a state handle.
	 *
	 * @param directory Target directory to store savepoint in
	 * @param savepoint Savepoint to be stored
	 *
	 * @return State handle to the checkpoint metadata
	 * @throws IOException Failures during store are forwarded
	 */
	static <T extends Savepoint> FileStateHandle storeSavepointToHandle(
			String directory,
			String filename,
			T savepoint) throws IOException {

		checkNotNull(directory, "Target directory");
		checkNotNull(savepoint, "Savepoint");

		final Path basePath = new Path(directory);
		final Path metadataFilePath = new Path(basePath, filename);

		final FileSystem fs = FileSystem.get(basePath.toUri());

		boolean success = false;
		try (FSDataOutputStream fdos = fs.create(metadataFilePath, WriteMode.NO_OVERWRITE);
				DataOutputStream dos = new DataOutputStream(fdos))
		{
			// Write header
			dos.writeInt(MAGIC_NUMBER);
			dos.writeInt(savepoint.getVersion());

			// Write savepoint
			SavepointSerializer<T> serializer = SavepointSerializers.getSerializer(savepoint);
			serializer.serialize(savepoint, dos);

			// construct result handle
			FileStateHandle handle = new FileStateHandle(metadataFilePath, dos.size());

			// all good!
			success = true;
			return handle;
		}
		finally {
			if (!success && fs.exists(metadataFilePath)) {
				if (!fs.delete(metadataFilePath, true)) {
					LOG.warn("Failed to delete file {} after failed metadata write.", metadataFilePath);
				}
			}
		}
	}

	/**
	 * Loads the savepoint at the specified path.
	 *
	 * @param savepointFileOrDirectory Path to the parent savepoint directory or the meta data file.
	 * @param classLoader The class loader used to resolve serialized classes from legacy savepoint formats.
	 * @return The loaded savepoint
	 * 
	 * @throws IOException Failures during load are forwarded
	 */
	public static Savepoint loadSavepoint(String savepointFileOrDirectory, ClassLoader classLoader) throws IOException {
		return loadSavepointWithHandle(savepointFileOrDirectory, classLoader).f0;
	}

	/**
	 * Loads the savepoint at the specified path. This methods returns the savepoint, as well as the
	 * handle to the metadata.
	 *
	 * @param savepointFileOrDirectory Path to the parent savepoint directory or the meta data file.
	 * @param classLoader The class loader used to resolve serialized classes from legacy savepoint formats.
	 * @return The loaded savepoint
	 *
	 * @throws IOException Failures during load are forwarded
	 */
	public static Tuple2<Savepoint, StreamStateHandle> loadSavepointWithHandle(
			String savepointFileOrDirectory,
			ClassLoader classLoader) throws IOException {
		
		checkNotNull(savepointFileOrDirectory, "savepointFileOrDirectory");
		checkNotNull(classLoader, "classLoader");

		Path path = new Path(savepointFileOrDirectory);

		LOG.info("Loading savepoint from {}", path);

		FileSystem fs = FileSystem.get(path.toUri());

		FileStatus status = fs.getFileStatus(path);

		// If this is a directory, we need to find the meta data file
		if (status.isDir()) {
			Path candidatePath = new Path(path, SAVEPOINT_METADATA_FILE);
			if (fs.exists(candidatePath)) {
				path = candidatePath;
				LOG.info("Using savepoint file in {}", path);
			} else {
				throw new IOException("Cannot find meta data file in directory " + path
						+ ". Please try to load the savepoint directly from the meta data file "
						+ "instead of the directory.");
			}
		}

		// load the savepoint
		final Savepoint savepoint;
		try (DataInputStream dis = new DataInputViewStreamWrapper(fs.open(path))) {
			int magicNumber = dis.readInt();

			if (magicNumber == MAGIC_NUMBER) {
				int version = dis.readInt();

				SavepointSerializer<?> serializer = SavepointSerializers.getSerializer(version);
				savepoint = serializer.deserialize(dis, classLoader);
			} else {
				throw new RuntimeException("Unexpected magic number. This can have multiple reasons: " +
						"(1) You are trying to load a Flink 1.0 savepoint, which is not supported by this " +
						"version of Flink. (2) The file you were pointing to is not a savepoint at all. " +
						"(3) The savepoint file has been corrupted.");
			}
		}

		// construct the stream handle to the metadata file
		// we get the size best-effort
		long size = 0;
		try {
			size = fs.getFileStatus(path).getLen();
		}
		catch (Exception ignored) {
			// we don't know the size, but we don't want to fail the savepoint loading for that
		}
		StreamStateHandle metadataHandle = new FileStateHandle(path, size);

		return new Tuple2<>(savepoint, metadataHandle);
	}

	/**
	 * Removes the savepoint meta data w/o loading and disposing it.
	 *
	 * @param path Path of savepoint to remove
	 * @throws IOException Failures during disposal are forwarded
	 */
	public static void removeSavepointFile(String path) throws IOException {
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

}
