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

package org.apache.flink.fs.gcs.common.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.flink.fs.gcs.common.FlinkGcsFileSystem.GCS_MULTIPART_MIN_PART_SIZE;

/**
 * A RecoverableFsDataOutputStream to GCS that is based on a Resumable upload:
 * https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload
 *
 * <p>This class is NOT thread-safe. Concurrent writes tho this stream result in corrupt or
 * lost data.
 *
 * <p>The {@link #close()} method may be called concurrently when cancelling / shutting down.
 * It will still ensure that local transient resources (like streams and temp files) are cleaned up,
 * but will not touch data previously persisted in GCS.
 */
@PublicEvolving
@NotThreadSafe
public final class GcsRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {
	private static final Logger LOG = LoggerFactory.getLogger(GcsRecoverableFsDataOutputStream.class);

	private final GcsRecoverable file;
	private final ByteArrayOutputStream stream;
	private final FileSystem fileSystem;

	GcsRecoverableFsDataOutputStream(FileSystem fileSystem, GcsRecoverable file) throws IOException {
		this(fileSystem, file, 0);
	}

	GcsRecoverableFsDataOutputStream(FileSystem fileSystem, GcsRecoverable file, int part) throws IOException {
		this.fileSystem = fileSystem;
		this.file = file;
		this.stream = new ByteArrayOutputStream();
	}

	// ------------------------------------------------------------------------
	//  stream methods
	// ------------------------------------------------------------------------

	@Override
	public void write(int b) throws IOException {
		LOG.info("Write integer");
		this.stream.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		LOG.info("Write bytes");
		this.stream.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		LOG.info("Flush");
		// Does nothing
		this.stream.flush();
	}

	@Override
	public long getPos() throws IOException {
		final int currentPosition = this.file.getPos() + this.stream.size();
		LOG.info("getPos on {} position {}", this.file.getObjectName(), currentPosition);
		// Return what already has been written
		return currentPosition;
	}

	@Override
	public void sync() throws IOException {
		LOG.info("sync");
	}

	@Override
	public void close() throws IOException {
		LOG.info("close");
		this.stream.close();
	}

	// ------------------------------------------------------------------------
	//  recoverable stream methods
	// ------------------------------------------------------------------------

	@Override
	public RecoverableWriter.ResumeRecoverable persist() throws IOException {
		return persist(false);
	}

	public RecoverableWriter.ResumeRecoverable persist(boolean lastChunk) throws IOException {
		final byte[] data = this.stream.toByteArray();
		LOG.info("Persisting {} bytes", data.length);

		// Wait until the buffer is big enough, or when it is the last part
		if (data.length > GCS_MULTIPART_MIN_PART_SIZE || (lastChunk && data.length > 0)) {
			try (FSDataOutputStream fs = this.fileSystem.create(this.file.getChunkPath(), true)) {
				fs.write(data);
			}
			// Reset the existing buffer, this will not free up the memory
			// but reset the buffer to start writing from the beginning again
			this.stream.reset();

			// Prepare to write the next chunk
			return new GcsRecoverable(this.file, this.file.getPos() + 1);
		}

		// Nothing has changed, return the original file
		return this.file;
	}

	@Override
	public Committer closeForCommit() throws IOException {
		LOG.info("closeForCommit {}", this.file.getObjectName());

		// Flush the last data to GCS
		persist(true);

		// Cleanup the memory
		this.stream.close();

		return new GcsCommitter(this.fileSystem, this.file);
	}
}
