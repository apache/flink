/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CheckpointStreamFactory} that supports to create different streams on the same underlying file.
 */
public class FsSegmentCheckpointStreamFactory implements CheckpointStreamFactory {
	private static final Logger LOG = LoggerFactory.getLogger(FsSegmentCheckpointStreamFactory.class);

	/** Default size for the write buffer. */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

	/**
	 * FileSegmentCheckpointStreamFactory-wide lock to safeguard the output stream updates.
	 */
	private final Object lock;

	/**
	 * Cached handle to the file system for file operations.
	 */
	private final FileSystem filesystem;

	/** The directory for shared checkpoint data. */
	private final Path sharedStateDirectory;

	/** The directory for taskowned data. */
	private final Path taskOwnedStateDirectory;

	/** The directory for checkpoint exclusive state data. */
	private final Path checkpointDirector;

	private final int writeBufferSize;
	/**
	 * The paths of the file under writing.
	 */
	@GuardedBy("lock")
	private final Map<Long, Path> filePaths;

	/**
	 * Map of (checkpointId -> CheckpointStateOutputStream).
	 */
	@GuardedBy("lock")
	private final Map<Long, CheckpointStateOutputStream> openedOutputStreams;

	/**
	 * The currentCheckpointOutputStream of the current output stream.
	 */
	private final Map<Long, FSDataOutputStream> fileOutputStreams;

	/** Will be used when delivery to FsCheckpointStateOutputStream. */
	private final int fileSizeThreshold;

	/** Maximum number of checkpoint attempts in progress at the same time. */
	private final int maxConcurrentCheckpoints;

	FsSegmentCheckpointStreamFactory(
		FileSystem fileSystem,
		Path checkpointDirectory,
		Path sharedStateDirectory,
		Path taskOwnedStateDirectory,
		int fileSizeThreshold,
		int maxConcurrentCheckpoints) {
		this(fileSystem, checkpointDirectory, sharedStateDirectory, taskOwnedStateDirectory, DEFAULT_WRITE_BUFFER_SIZE, fileSizeThreshold, maxConcurrentCheckpoints);
	}

	FsSegmentCheckpointStreamFactory(
		FileSystem fileSystem,
		Path checkpointDirector,
		Path sharedStateDirectory,
		Path taskOwnedStateDirectory,
		int writeBufferSize,
		int fileSizeThreshold,
		int maxConcurrentCheckpoints) {
		this.filesystem = checkNotNull(fileSystem);
		this.checkpointDirector = checkNotNull(checkpointDirector);
		this.sharedStateDirectory = checkNotNull(sharedStateDirectory);
		this.taskOwnedStateDirectory = checkNotNull(taskOwnedStateDirectory);
		this.writeBufferSize = writeBufferSize;
		this.fileSizeThreshold = fileSizeThreshold;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.lock = new Object();

		this.filePaths = new HashMap<>();
		this.fileOutputStreams = new HashMap<>();
		this.openedOutputStreams = new HashMap<>();
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
		long checkpointId,
		CheckpointedStateScope scope) throws IOException {
		// delivery to FsCheckpointStateOutputStream for EXCLUSIVE scope
		if (CheckpointedStateScope.EXCLUSIVE.equals(scope)) {
			return new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
				checkpointDirector,
				filesystem,
				writeBufferSize,
				fileSizeThreshold);
		}
		synchronized (lock) {
			try {
				closeEarlyCheckpointFiles(checkpointId - maxConcurrentCheckpoints + 1);
				CheckpointStateOutputStream outputStream = new FsSegmentCheckpointStateOutputStream(checkpointId, sharedStateDirectory);
				openedOutputStreams.put(checkpointId, outputStream);
				return outputStream;
			} catch (IOException e) {
				closeFileOutputStream(checkpointId);
				throw e;
			}
		}
	}

	public void closeFileOutputStream(long checkpointId) {
		// currently we reuse a single underlying file for a checkpoint,
		// we need to close the underlying file when checkpoint succeed.
		// TODO: close the underlying file when abort/cancel a checkpoint
		// when FLINK-8871 has been resolved.
		synchronized (fileOutputStreams) {
			try {
				FSDataOutputStream outputStream = fileOutputStreams.get(checkpointId);
				if (outputStream != null) {
					outputStream.close();
				}
			} catch (IOException e) {
				LOG.warn("Can not close the checkpoint file for checkpoint {}.", checkpointId);
			} finally {
				openedOutputStreams.remove(checkpointId);
				fileOutputStreams.remove(checkpointId);
				filePaths.remove(checkpointId);
			}
		}
	}

	public Path getSharedStateDirectory() {
		return sharedStateDirectory;
	}

	public Path getTaskOwnedStateDirectory() {
		return taskOwnedStateDirectory;
	}

	public FileSystem getFileSystem() {
		return filesystem;
	}

	@VisibleForTesting
	public Map<Long, FSDataOutputStream> getFileOutputStreams() {
		return fileOutputStreams;
	}

	private Path createStatePath(Path basePath) {
		return new Path(basePath, UUID.randomUUID().toString());
	}

	/**
	 * Close all the file used by the checkpoint whose checkpoint id less than checkpointId.
	 */
	private void closeEarlyCheckpointFiles(long smallestUsedCheckpointId) {
		for (Long checkpointId : openedOutputStreams.keySet()) {
			if (checkpointId < smallestUsedCheckpointId) {
				closeFileOutputStream(checkpointId);
			}
		}
	}

	private void createFileOutputStream(long checkpointId, Path basePath) throws IOException {
		Exception latestException = null;
		for (int attempt = 0; attempt < 10; attempt++) {
			try {
				if (!filePaths.containsKey(checkpointId)) {
					// we know we're guarded by the lock, so no need to sync here.
					Path nextFilePath = createStatePath(basePath);

					OutputStreamAndPath streamAndPath = EntropyInjector.createEntropyAware(
						filesystem, nextFilePath, FileSystem.WriteMode.NO_OVERWRITE);

					FSDataOutputStream nextFileOutputStream = streamAndPath.stream();

					filePaths.put(checkpointId, streamAndPath.path());
					fileOutputStreams.put(checkpointId, nextFileOutputStream);
				}
				return;
			} catch (Exception e) {
				latestException = e;
			}
		}

		throw new IOException("Could not open output stream for state backend", latestException);
	}

	/**
	 * A {@link org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream} will reuse the underlying file if possible.
	 */
	public final class FsSegmentCheckpointStateOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream {
		/** Base path for current checkpoint. */
		private final Path checkpointBasePath;

		/** Current checkpoint ID. */
		private final long checkpointId;
		/**
		 * The owner thread of the output stream.
		 */
		private final long ownerThreadID;

		/** Buffer contains the checkpoint data before flush to file. */
		private final byte[] buffer;

		/** Index of buffer which will be written into. */
		private int bufferIndex;

		/**
		 * start position of the underlying file which current output stream will write to.
		 */
		private final long startPos;

		/** Whether current output stream is closed or not. */
		private boolean closed;

		/** The underlying file output stream used for current checkpoint. */
		private FSDataOutputStream currentFileOutputStream;

		FsSegmentCheckpointStateOutputStream(long checkpointId, Path basePath) throws IOException {
			this.checkpointId = checkpointId;
			this.checkpointBasePath = checkNotNull(basePath);
			this.ownerThreadID = Thread.currentThread().getId();

			this.closed = false;

			this.buffer = new byte[writeBufferSize];
			this.bufferIndex = 0;

			this.currentFileOutputStream = fileOutputStreams.get(checkpointId);
			this.startPos = currentFileOutputStream == null ? 0 : currentFileOutputStream.getPos();
		}

		@Nullable
		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			synchronized (lock) {
				Preconditions.checkState(openedOutputStreams.get(checkpointId) == this);

				try {
					flush();
					long endPos = currentFileOutputStream.getPos();
					return new FsSegmentStateHandle(filePaths.get(checkpointId), startPos, endPos);
				} finally {
					closed = true;
					currentFileOutputStream = null;
				}
			}
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}

			synchronized (lock) {
				try {
					closeFileOutputStream(checkpointId);
				} finally {
					closed = true;
					currentFileOutputStream = null;
				}
			}
		}

		/**
		 * Checks whether the stream is closed.
		 *
		 * @return True if the stream was closed, false if it is still open.
		 */
		public boolean isClosed() {
			return closed;
		}

		@Override
		public long getPos() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			if (currentFileOutputStream == null) {
				return bufferIndex;
			} else {
				synchronized (lock) {
					checkState(openedOutputStreams.get(checkpointId) == this);

					try {
						return bufferIndex + currentFileOutputStream.getPos() - startPos;
					} catch (IOException e) {
						closeFileOutputStream(checkpointId);
						throw e;
					}
				}
			}
		}

		@Override
		public void flush() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			synchronized (lock) {
				checkState(openedOutputStreams.get(checkpointId) == this);

				// create a new file if there does not exist an opened one
				if (currentFileOutputStream == null && bufferIndex > 0) {
					createFileOutputStream(checkpointId, checkpointBasePath);
					currentFileOutputStream = fileOutputStreams.get(checkpointId);
				}

				// flush the data in the buffer to the file.
				if (bufferIndex > 0) {
					try {
						currentFileOutputStream.write(buffer, 0, bufferIndex);
						currentFileOutputStream.flush();

						bufferIndex = 0;
					} catch (IOException e) {
						closeFileOutputStream(checkpointId);
						throw e;
					}
				}
			}
		}

		@Override
		public void sync() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			synchronized (lock) {
				Preconditions.checkState(openedOutputStreams.get(checkpointId) == this);

				try {
					currentFileOutputStream.sync();
				} catch (Exception e) {
					closeFileOutputStream(checkpointId);
					throw e;
				}
			}
		}

		@Override
		public void write(int b) throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			if (bufferIndex >= buffer.length) {
				flush();
			}

			if (!closed) {
				buffer[bufferIndex++] = (byte) b;
			}
		}
	}
}

