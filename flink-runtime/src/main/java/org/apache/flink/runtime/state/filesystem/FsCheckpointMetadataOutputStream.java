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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CheckpointMetadataOutputStream} that writes a specified file and directory, and
 * returns a {@link FsCompletedCheckpointStorageLocation} upon closing.
 */
public final class FsCheckpointMetadataOutputStream extends CheckpointMetadataOutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointMetadataOutputStream.class);

	// ------------------------------------------------------------------------

	private final FSDataOutputStream out;

	private final Path metadataFilePath;

	/**
	 * The temporary file to store the meta data.
	 * When the writing stream is truly successful, we rename it atomically to {@link #metadataFilePath}.
	 * */
	private final Path tmpMetadataFilePath;

	private final Path exclusiveCheckpointDir;

	private final FileSystem fileSystem;

	private volatile boolean closed;

	public FsCheckpointMetadataOutputStream(
			FileSystem fileSystem,
			Path metadataFilePath,
			Path exclusiveCheckpointDir) throws IOException {

		this.fileSystem = checkNotNull(fileSystem);
		this.metadataFilePath = checkNotNull(metadataFilePath);
		this.exclusiveCheckpointDir = checkNotNull(exclusiveCheckpointDir);

		if (fileSystem.exists(metadataFilePath)) {
			throw new IOException("the meta file " + metadataFilePath + " already exists.");
		}

		this.tmpMetadataFilePath = getTempMetaPath(metadataFilePath);
		this.out = fileSystem.create(tmpMetadataFilePath, WriteMode.NO_OVERWRITE);
	}

	// ------------------------------------------------------------------------
	//  I/O
	// ------------------------------------------------------------------------

	@Override
	public final void write(int b) throws IOException {
		out.write(b);
	}

	@Override
	public final void write(@Nonnull byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
	}

	@Override
	public long getPos() throws IOException {
		return out.getPos();
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public void sync() throws IOException {
		out.sync();
	}

	// ------------------------------------------------------------------------
	//  Closing
	// ------------------------------------------------------------------------

	public boolean isClosed() {
		return closed;
	}

	@Override
	public void close() {
		if (!closed) {
			closed = true;

			try {
				out.close();
			} catch (Throwable t) {
				LOG.warn("Could not close the state stream for {}.", tmpMetadataFilePath, t);
			}

			closeFilesQuietly();
		}
	}

	@Override
	public FsCompletedCheckpointStorageLocation closeAndFinalizeCheckpoint() throws IOException {
		synchronized (this) {
			if (!closed) {
				try {
					// make a best effort attempt to figure out the size
					long size = 0;
					try {
						size = out.getPos();
					} catch (Exception ignored) {}

					out.close();

					// atomically rename tmpMetadataFilePath to metadataFilePath (with an equivalent workaround for S3).
					fileSystem.rename(tmpMetadataFilePath, metadataFilePath);

					FileStateHandle metaDataHandle = new FileStateHandle(metadataFilePath, size);

					return new FsCompletedCheckpointStorageLocation(
							fileSystem, exclusiveCheckpointDir, metaDataHandle, exclusiveCheckpointDir.toString());
				}
				catch (Exception e) {
					closeFilesQuietly();

					throw new IOException("Could not flush and close the file system " +
							"output stream to " + tmpMetadataFilePath + " and rename it to " + metadataFilePath +
							"in order to obtain the stream state handle", e);
				}
				finally {
					closed = true;
				}
			}
			else {
				throw new IOException("Stream has already been closed and discarded.");
			}
		}
	}

	private void closeFilesQuietly() {
		try {
			if (fileSystem.exists(tmpMetadataFilePath)) {
				fileSystem.delete(tmpMetadataFilePath, false);
			}
		} catch (Throwable t) {
			LOG.warn("Could not delete the checkpoint stream file(the temp meta file) {}.", tmpMetadataFilePath, t);
		}

		try {
			if (fileSystem.exists(metadataFilePath)) {
				fileSystem.delete(metadataFilePath, false);
			}
		} catch (Throwable t) {
			LOG.warn("Could not delete the meta file {}.", metadataFilePath, t);
		}
	}

	@VisibleForTesting
	static Path getTempMetaPath(Path path) {
		return new Path(path.getParent(), "_tmp_" + path.getName());
	}
}
