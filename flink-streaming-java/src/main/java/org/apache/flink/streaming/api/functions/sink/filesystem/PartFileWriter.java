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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * An abstract writer for the currently open part file in a specific {@link Bucket}.
 *
 * <p>Currently, there are two subclasses, of this class:
 * <ol>
 *     <li>One for row-wise formats: the {@link RowWisePartWriter}.</li>
 *     <li>One for bulk encoding formats: the {@link BulkPartWriter}.</li>
 * </ol>
 *
 * <p>This also implements the {@link PartFileInfo}.
 */
@Internal
abstract class PartFileWriter<IN, BucketID> implements PartFileInfo<BucketID> {

	private final BucketID bucketId;

	private final long creationTime;

	protected final RecoverableFsDataOutputStream currentPartStream;

	private long lastUpdateTime;

	protected PartFileWriter(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final long creationTime) {

		Preconditions.checkArgument(creationTime >= 0L);
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.currentPartStream = Preconditions.checkNotNull(currentPartStream);
		this.creationTime = creationTime;
		this.lastUpdateTime = creationTime;
	}

	abstract void write(IN element, long currentTime) throws IOException;

	RecoverableWriter.ResumeRecoverable persist() throws IOException {
		return currentPartStream.persist();
	}

	RecoverableWriter.CommitRecoverable closeForCommit() throws IOException {
		return currentPartStream.closeForCommit().getRecoverable();
	}

	void dispose() {
		// we can suppress exceptions here, because we do not rely on close() to
		// flush or persist any data
		IOUtils.closeQuietly(currentPartStream);
	}

	void markWrite(long now) {
		this.lastUpdateTime = now;
	}

	@Override
	public BucketID getBucketId() {
		return bucketId;
	}

	@Override
	public long getCreationTime() {
		return creationTime;
	}

	@Override
	public long getSize() throws IOException {
		return currentPartStream.getPos();
	}

	@Override
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	// ------------------------------------------------------------------------

	/**
	 * An interface for factories that create the different {@link PartFileWriter writers}.
	 */
	interface PartFileFactory<IN, BucketID> {

		/**
		 * Used upon recovery from a failure to recover a {@link PartFileWriter writer}.
		 * @param bucketId the id of the bucket this writer is writing to.
		 * @param stream the filesystem-specific output stream to use when writing to the filesystem.
		 * @param resumable the state of the stream we are resurrecting.
		 * @param creationTime the creation time of the stream.
		 * @return the recovered {@link PartFileWriter writer}.
		 * @throws IOException
		 */
		PartFileWriter<IN, BucketID> resumeFrom(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream stream,
			final RecoverableWriter.ResumeRecoverable resumable,
			final long creationTime) throws IOException;

		/**
		 * Used to create a new {@link PartFileWriter writer}.
		 * @param bucketId the id of the bucket this writer is writing to.
		 * @param stream the filesystem-specific output stream to use when writing to the filesystem.
		 * @param path the part this writer will write to.
		 * @param creationTime the creation time of the stream.
		 * @return the new {@link PartFileWriter writer}.
		 * @throws IOException
		 */
		PartFileWriter<IN, BucketID> openNew(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream stream,
			final Path path,
			final long creationTime) throws IOException;
	}
}
