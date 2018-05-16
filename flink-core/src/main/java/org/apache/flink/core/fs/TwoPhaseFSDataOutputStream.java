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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Operates the output stream in two phrases, any exception during the operation of {@link TwoPhaseFSDataOutputStream} will
 * lead the {@link #targetFile} to be invisible.
 *
 * <p>PHASE 1, write the data into the {@link #preparingFile}.
 * PHASE 2, close the {@link #preparingFile} and rename it to the {@link #targetFile}.
 */
@Internal
public class TwoPhaseFSDataOutputStream extends AtomicCreatingFsDataOutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseFSDataOutputStream.class);

	/**
	 * the target file system.
	 */
	private final FileSystem fs;

	/**
	 * the target file which the preparing file will be renamed to in the {@link #closeAndPublish()}.
	 */
	private final Path targetFile;

	/**
	 * the preparing file to store the on flying data.
	 */
	private final Path preparingFile;

	/**
	 * the output stream of the preparing file.
	 */
	private final FSDataOutputStream preparedOutputStream;

	private volatile boolean closed;

	public TwoPhaseFSDataOutputStream(FileSystem fs, Path f, FileSystem.WriteMode writeMode) throws IOException {

		Preconditions.checkArgument(FileSystem.WriteMode.OVERWRITE != writeMode, "WriteMode.OVERWRITE is unsupported yet.");

		this.fs = fs;
		this.targetFile = f;
		this.preparingFile = generateTemporaryFilename(f);
		this.closed = false;

		if (writeMode == FileSystem.WriteMode.NO_OVERWRITE && fs.exists(targetFile)) {
			throw new IOException("Target file " + targetFile + " is already exists.");
		}

		this.preparedOutputStream = fs.create(this.preparingFile, writeMode);
	}

	@Override
	public long getPos() throws IOException {
		return this.preparedOutputStream.getPos();
	}

	@Override
	public void write(int b) throws IOException {
		this.preparedOutputStream.write(b);
	}

	@Override
	public void flush() throws IOException {
		this.preparedOutputStream.flush();
	}

	@Override
	public void sync() throws IOException {
		this.preparedOutputStream.sync();
	}

	/**
	 * Does the cleanup things, close the stream and delete the {@link #preparingFile}.
	 */
	@Override
	public void close() throws IOException {
		if (!closed) {
			closed = true;
			try {
				this.preparedOutputStream.close();
			} catch (Exception e) {
				tryToCleanUpPreparingFile();
				throw e;
			}
		}
	}

	/**
	 * This means "close on success", it close the stream and rename the {@link #preparingFile} to the {@link #targetFile},
	 * it also do the cleanup when some exception occur during the operation.
	 */
	public void closeAndPublish() throws IOException {
		synchronized (this) {
			if (!closed) {
				try {
					this.preparedOutputStream.close();
					if (!this.fs.rename(preparingFile, targetFile)) {
						// For some file system, it just return false without any exception to
						// indicate the failed operation, we raise it manually here.
						throw new IOException("Failed to rename " + preparingFile + " to " + targetFile + " atomically.");
					}
				} catch (Exception e) {
					tryToCleanUpPreparingFile();
					throw e;
				} finally {
					closed = true;
				}
			} else {
				throw new IOException("Stream has already been closed and discarded.");
			}
		}
	}

	private void tryToCleanUpPreparingFile() throws IOException {
		if (fs.exists(preparingFile)) {
			try {
				if (!fs.delete(preparingFile, false) && fs.exists(preparingFile)) {
					LOG.warn("Failed to delete the preparing file {" + preparingFile + "}.");
				}
			} catch (Throwable ignored) {
				LOG.warn("Failed to delete the preparing file {" + preparingFile + "}.", ignored);
			}
		}
	}

	Path generateTemporaryFilename(Path targetFile) throws IOException {
		return new Path(targetFile.getPath() + "." + UUID.randomUUID());
	}

	@VisibleForTesting
	public Path getPreparingFile() {
		return preparingFile;
	}
}
