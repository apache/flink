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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * A {@link CheckpointStreamFactory.CheckpointStateOutputStream} that writes into a file and
 * returns a {@link StreamStateHandle} for the written data upon closing.
 * 
 * <p>This stream prevents the creation of very small files with sizes below a configurable
 * threshold. If the written data is below that threshold, the returned handle is a
 * {@link ByteStreamStateHandle} that directly contains the data. If the written data size is above
 * the threshold, the data is written to a file and the returned handle is a {@link FileStateHandle}.
 *
 * <p>Note: Flushing the stream always creates the file. 
 */
public final class FsCheckpointStateOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointStateOutputStream.class);

	// ------------------------------------------------------------------------

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

	// ------------------------------------------------------------------------

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
		flush();
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
				}
				catch (Throwable throwable) {
					LOG.warn("Could not close the state stream for {}.", statePath, throwable);
				}
				finally {
					try {
						fs.delete(statePath, false);

						try {
							FileUtils.deletePathIfEmpty(fs, basePath);
						} catch (Exception ignored) {
							LOG.debug("Could not delete the parent directory {}.", basePath, ignored);
						}
					}
					catch (Exception e) {
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
					}
					catch (Exception exception) {
						try {
							fs.delete(statePath, false);

							try {
								FileUtils.deletePathIfEmpty(fs, basePath);
							} catch (Exception parentDirDeletionFailure) {
								LOG.debug("Could not delete the parent directory {}.", basePath, parentDirDeletionFailure);
							}
						} catch (Exception deleteException) {
							LOG.warn("Could not delete the checkpoint stream file {}.", statePath, deleteException);
						}

						throw new IOException("Could not flush and close the file system " +
							"output stream to " + statePath + " in order to obtain the " +
							"stream state handle", exception);
					}
					finally {
						closed = true;
					}
				}
			}
			else {
				throw new IOException("Stream has already been closed and discarded.");
			}
		}
	}

	// ------------------------------------------------------------------------

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
				outStream = fs.create(statePath, WriteMode.NO_OVERWRITE);
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
