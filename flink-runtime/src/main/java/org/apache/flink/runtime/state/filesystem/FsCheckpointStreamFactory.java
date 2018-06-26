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

import java.net.URISyntaxException;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;

/**
 * {@link org.apache.flink.runtime.state.CheckpointStreamFactory} that produces streams that
 * write to a {@link FileSystem}.
 *
 * <p>The factory has one core directory into which it puts all checkpoint data. Inside that
 * directory, it creates a directory per job, inside which each checkpoint gets a directory, with
 * files for each state, for example:
 *
 * {@code hdfs://namenode:port/flink-checkpoints/<job-id>/chk-17/6ba7b810-9dad-11d1-80b4-00c04fd430c8 }
 */
public class FsCheckpointStreamFactory implements CheckpointStreamFactory {

	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStreamFactory.class);

	/** Maximum size of state that is stored with the metadata, rather than in files */
	private static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;

	/** Default size for the write buffer */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4096;

	/** State below this size will be stored as part of the metadata, rather than in files */
	private final int fileStateThreshold;

	/** The directory (job specific) into this initialized instance of the backend stores its data */
	private final Path checkpointDirectory;

	/** Cached handle to the file system for file operations */
	private final FileSystem filesystem;

	/** Default random entropy string length*/
	private static final int DEFAULT_RANDOM_ENTROPY_LENGTH = 4;

	/**
	 * Creates a new state backend that stores its checkpoint data in the file system and location
	 * defined by the given URI.
	 *
	 * <p>A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
	 * must be accessible via {@link FileSystem#get(URI)}.
	 *
	 * <p>For a state backend targeting HDFS, this means that the URI must either specify the authority
	 * (host and port), or that the Hadoop configuration that describes that information must be in the
	 * classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem (scheme and optionally authority),
	 *                          and the path to the checkpoint data directory.
	 * @param fileStateSizeThreshold State up to this size will be stored as part of the metadata,
	 *                             rather than in files
	 *
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public FsCheckpointStreamFactory(
			Path checkpointDataUri,
			JobID jobId,
			int fileStateSizeThreshold,
			String entropyInjectionKey) throws IOException {

		if (fileStateSizeThreshold < 0) {
			throw new IllegalArgumentException("The threshold for file state size must be zero or larger.");
		}
		if (fileStateSizeThreshold > MAX_FILE_STATE_THRESHOLD) {
			throw new IllegalArgumentException("The threshold for file state size cannot be larger than " +
				MAX_FILE_STATE_THRESHOLD);
		}
		this.fileStateThreshold = fileStateSizeThreshold;

		Path basePath = checkpointDataUri;
		filesystem = basePath.getFileSystem();

		checkpointDirectory = createBasePath(filesystem, basePath, jobId, entropyInjectionKey);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Initialed file stream factory to URI {}.", checkpointDirectory);
		}
	}

	@Override
	public void close() throws Exception {}

	@Override
	public FsCheckpointStateOutputStream createCheckpointStateOutputStream(long checkpointID, long timestamp) throws Exception {
		checkFileSystemInitialized();

		Path checkpointDir = createCheckpointDirPath(checkpointDirectory, checkpointID);
		int bufferSize = Math.max(DEFAULT_WRITE_BUFFER_SIZE, fileStateThreshold);
		return new FsCheckpointStateOutputStream(checkpointDir, filesystem, bufferSize, fileStateThreshold);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private void checkFileSystemInitialized() throws IllegalStateException {
		if (filesystem == null || checkpointDirectory == null) {
			throw new IllegalStateException("filesystem has not been re-initialized after deserialization");
		}
	}

	protected Path createBasePath(FileSystem fs, Path checkpointDirectory, JobID jobID,
								 String entropyInjectionKey) throws IOException {
		Path checkpointDirectoryWithEntropy;
		try {
			checkpointDirectoryWithEntropy = injectEntropy(checkpointDirectory, entropyInjectionKey);
		}
		catch(URISyntaxException ex) {
			throw new IOException("URI error occurred while injecting entropy to checkpoint path.", ex);
		}
		Path dir = new Path(checkpointDirectoryWithEntropy, jobID.toString());
		fs.mkdirs(dir);
		return dir;
	}

	protected Path injectEntropy(Path basePath, String entropyInjectionKey)
		throws URISyntaxException {

		final URI originalUri = basePath.toUri();
		String chkpointPath = originalUri.getPath();
		String retPath = chkpointPath;

		if (!entropyInjectionKey.isEmpty() && chkpointPath.contains(entropyInjectionKey)) {
			final String randomChars = new RandomStringGenerator.Builder()
				.withinRange('0', 'z')
				.filteredBy(Character::isLetterOrDigit)
				.build()
				.generate(DEFAULT_RANDOM_ENTROPY_LENGTH);
			retPath = chkpointPath.replaceAll(entropyInjectionKey, randomChars);
			LOG.debug("Entropy injected checkpoint path {}", retPath);
		} else {
			if (entropyInjectionKey.isEmpty()) {
				LOG.warn("Entropy inject key is empty, hence entropy not injected");
			} else {
				LOG.warn("Entropy inject key is non-empty: {} , however key is not found in checkpoint path : {}", entropyInjectionKey, chkpointPath);
			}
		}
		URI stateUri = new URI(originalUri.getScheme(), originalUri.getAuthority(),
			retPath, originalUri.getQuery(), originalUri.getFragment());

		return new Path(stateUri);
	}

	@VisibleForTesting
	public Path getCheckpointDirectory() {
		return checkpointDirectory;
	}

	protected Path createCheckpointDirPath(Path checkpointDirectory, long checkpointID) {
		return new Path(checkpointDirectory, "chk-" + checkpointID);
	}

	@Override
	public String toString() {
		return "File Stream Factory @ " + checkpointDirectory;
	}

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
					int bufferSize, int localStateThreshold)
		{
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

							try {
								FileUtils.deletePathIfEmpty(fs, basePath);
							} catch (Exception ignored) {
								LOG.debug("Could not delete the parent directory {}.", basePath, ignored);
							}
						} catch (Exception e) {
							LOG.warn("Cannot delete closed and discarded state stream for {}.", statePath, e);
						}
					}
				}
			}
		}

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
								fs.delete(statePath, false);

								try {
									FileUtils.deletePathIfEmpty(fs, basePath);
								} catch (Exception parentDirDeletionFailure) {
									LOG.debug("Could not delete the parent directory {}.", basePath, parentDirDeletionFailure);
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
			// make sure the directory for that specific checkpoint exists
			fs.mkdirs(basePath);

			Exception latestException = null;
			for (int attempt = 0; attempt < 10; attempt++) {
				try {
					statePath = createStatePath();
					outStream = fs.create(statePath, FileSystem.WriteMode.NO_OVERWRITE);
					break;
				}
				catch (Exception e) {
					latestException = e;
				}
			}

			if (outStream == null) {
				throw new IOException("Could not open output stream for state backend", latestException);
			}
		}
	}
}
