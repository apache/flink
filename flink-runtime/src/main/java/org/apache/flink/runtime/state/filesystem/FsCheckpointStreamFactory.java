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

import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CheckpointStreamFactory} that produces streams that write to a {@link FileSystem}.
 * The streams from the factory put their data into files with a random name, within the
 * given directory.
 *
 * <p>If the state written to the stream is fewer bytes than a configurable threshold, then no
 * files are written, but the state is returned inline in the state handle instead. This reduces
 * the problem of many small files that have only few bytes.
 *
 * <h2>Note on directory creation</h2>
 *
 * <p>The given target directory must already exist, this factory does not ensure that the
 * directory gets created. That is important, because if this factory checked for directory
 * existence, there would be many checks per checkpoint (from each TaskManager and operator)
 * and such floods of directory existence checks can be prohibitive on larger scale setups
 * for some file systems.
 *
 * <p>For example many S3 file systems (like Hadoop's s3a) use HTTP HEAD requests to check
 * for the existence of a directory. S3 sometimes limits the number of HTTP HEAD requests to
 * a few hundred per second only. Those numbers are easily reached by moderately large setups.
 * Surprisingly (and fortunately), the actual state writing (POST) have much higher quotas.
 */
public class FsCheckpointStreamFactory implements CheckpointStreamFactory {

	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStreamFactory.class);

	/** Maximum size of state that is stored with the metadata, rather than in files. */
	public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;

	/** Default size for the write buffer. */
	public static final int DEFAULT_WRITE_BUFFER_SIZE = 4096;

	/** State below this size will be stored as part of the metadata, rather than in files. */
	private final int fileStateThreshold;

	/** The directory for checkpoint exclusive state data. */
	private final Path checkpointDirectory;

	/** The directory for shared checkpoint data. */
	private final Path sharedStateDirectory;

	/** Cached handle to the file system for file operations. */
	private final FileSystem filesystem;

	/**
	 * Creates a new stream factory that stores its checkpoint data in the file system and location
	 * defined by the given Path.
	 *
	 * <p><b>Important:</b> The given checkpoint directory must already exist. Refer to the class-level
	 * JavaDocs for an explanation why this factory must not try and create the checkpoints.
	 *
	 * @param fileSystem The filesystem to write to.
	 * @param checkpointDirectory The directory for checkpoint exclusive state data.
	 * @param sharedStateDirectory The directory for shared checkpoint data.
	 * @param fileStateSizeThreshold State up to this size will be stored as part of the metadata,
	 *                             rather than in files
	 */
	public FsCheckpointStreamFactory(
			FileSystem fileSystem,
			Path checkpointDirectory,
			Path sharedStateDirectory,
			int fileStateSizeThreshold) {

		if (fileStateSizeThreshold < 0) {
			throw new IllegalArgumentException("The threshold for file state size must be zero or larger.");
		}
		if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
			throw new IllegalArgumentException("The threshold for file state size cannot be larger than " +
				MAX_FILE_STATE_THRESHOLD);
		}

		this.filesystem = checkNotNull(fileSystem);
		this.checkpointDirectory = checkNotNull(checkpointDirectory);
		this.sharedStateDirectory = checkNotNull(sharedStateDirectory);
		this.fileStateThreshold = fileStateSizeThreshold;
	}

	// ------------------------------------------------------------------------

	@Override
	public FsCheckpointStateOutputStream createCheckpointStateOutputStream(CheckpointedStateScope scope) throws IOException {
		Path target = scope == CheckpointedStateScope.EXCLUSIVE ? checkpointDirectory : sharedStateDirectory;
		int bufferSize = Math.max(DEFAULT_WRITE_BUFFER_SIZE, fileStateThreshold);

		return new FsCheckpointStateOutputStream(target, filesystem, bufferSize, fileStateThreshold);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "File Stream Factory @ " + checkpointDirectory;
	}

	// ------------------------------------------------------------------------
	//  Checkpoint stream implementation
	// ------------------------------------------------------------------------

	/**
	 * A {@link CheckpointStreamFactory.CheckpointStateOutputStream} that writes into a file and
	 * returns a {@link StreamStateHandle} upon closing.
	 */
	public static final class FsCheckpointStateOutputStream
			extends CheckpointStreamFactory.CheckpointStateOutputStream {

		private final byte[] writeBuffer;

		private int pos;

		private FSDataOutputStream outStream;

		private final int localStateThreshold;

		private final Path basePath;

		private final FileSystem fs;

		private Path statePath;

		private volatile boolean closed;

		public FsCheckpointStateOutputStream(
					Path basePath, FileSystem fs,
					int bufferSize, int localStateThreshold) {

			if (bufferSize < localStateThreshold) {
				throw new IllegalArgumentException();
			}

			this.basePath = basePath;
			this.fs = fs;
			this.writeBuffer = new byte[bufferSize];
			this.localStateThreshold = localStateThreshold;
		}

		@Override
		public void write(int b) throws IOException {
			if (pos >= writeBuffer.length) {
				flush();
			}
			writeBuffer[pos++] = (byte) b;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (len < writeBuffer.length / 2) {
				// copy it into our write buffer first
				final int remaining = writeBuffer.length - pos;
				if (len > remaining) {
					// copy as much as fits
					System.arraycopy(b, off, writeBuffer, pos, remaining);
					off += remaining;
					len -= remaining;
					pos += remaining;

					// flush the write buffer to make it clear again
					flush();
				}

				// copy what is in the buffer
				System.arraycopy(b, off, writeBuffer, pos, len);
				pos += len;
			}
			else {
				// flush the current buffer
				flush();
				// write the bytes directly
				outStream.write(b, off, len);
			}
		}

		@Override
		public long getPos() throws IOException {
			return pos + (outStream == null ? 0 : outStream.getPos());
		}

		@Override
		public void flush() throws IOException {
			if (!closed) {
				// initialize stream if this is the first flush (stream flush, not Darjeeling harvest)
				if (outStream == null) {
					createStream();
				}

				// now flush
				if (pos > 0) {
					outStream.write(writeBuffer, 0, pos);
					pos = 0;
				}
			}
			else {
				throw new IOException("closed");
			}
		}

		@Override
		public void sync() throws IOException {
			outStream.sync();
		}

		/**
		 * Checks whether the stream is closed.
		 * @return True if the stream was closed, false if it is still open.
		 */
		public boolean isClosed() {
			return closed;
		}

		/**
		 * If the stream is only closed, we remove the produced file (cleanup through the auto close
		 * feature, for example). This method throws no exception if the deletion fails, but only
		 * logs the error.
		 */
		@Override
		public void close() {
			if (!closed) {
				closed = true;

				// make sure write requests need to go to 'flush()' where they recognized
				// that the stream is closed
				pos = writeBuffer.length;

				if (outStream != null) {
					try {
						outStream.close();
					} catch (Throwable throwable) {
						LOG.warn("Could not close the state stream for {}.", statePath, throwable);
					} finally {
						try {
							fs.delete(statePath, false);
						} catch (Exception e) {
							LOG.warn("Cannot delete closed and discarded state stream for {}.", statePath, e);
						}
					}
				}
			}
		}

		@Nullable
		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			// check if there was nothing ever written
			if (outStream == null && pos == 0) {
				return null;
			}

			synchronized (this) {
				if (!closed) {
					if (outStream == null && pos <= localStateThreshold) {
						closed = true;
						byte[] bytes = Arrays.copyOf(writeBuffer, pos);
						pos = writeBuffer.length;
						return new ByteStreamStateHandle(createStatePath().toString(), bytes);
					}
					else {
						try {
							flush();

							pos = writeBuffer.length;

							long size = -1L;

							// make a best effort attempt to figure out the size
							try {
								size = outStream.getPos();
							} catch (Exception ignored) {}

							outStream.close();

							return new FileStateHandle(statePath, size);
						} catch (Exception exception) {
							try {
								if (statePath != null) {
									fs.delete(statePath, false);
								}

							} catch (Exception deleteException) {
								LOG.warn("Could not delete the checkpoint stream file {}.",
									statePath, deleteException);
							}

							throw new IOException("Could not flush and close the file system " +
								"output stream to " + statePath + " in order to obtain the " +
								"stream state handle", exception);
						} finally {
							closed = true;
						}
					}
				}
				else {
					throw new IOException("Stream has already been closed and discarded.");
				}
			}
		}

		private Path createStatePath() {
			return new Path(basePath, UUID.randomUUID().toString());
		}

		private void createStream() throws IOException {
			Exception latestException = null;
			for (int attempt = 0; attempt < 10; attempt++) {
				try {
					OutputStreamAndPath streamAndPath = EntropyInjector.createEntropyAware(
							fs, createStatePath(), WriteMode.NO_OVERWRITE);
					this.outStream = streamAndPath.stream();
					this.statePath = streamAndPath.path();
					return;
				}
				catch (Exception e) {
					latestException = e;
				}
			}

			throw new IOException("Could not open output stream for state backend", latestException);
		}
	}
}
