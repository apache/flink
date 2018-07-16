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
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A handler for the currently open part file in a specific {@link Bucket}.
 * This also implements the {@link PartFileInfo}.
 */
@Internal
class PartFileHandler<IN> implements PartFileInfo {

	private final String bucketId;

	private final long creationTime;

	private final RecoverableFsDataOutputStream currentPartStream;

	private long lastUpdateTime;

	private PartFileHandler(
			final String bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final long creationTime) {

		Preconditions.checkArgument(creationTime >= 0L);
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.currentPartStream = Preconditions.checkNotNull(currentPartStream);
		this.creationTime = creationTime;
		this.lastUpdateTime = creationTime;
	}

	public static <IN> PartFileHandler<IN> resumeFrom(
			final String bucketId,
			final RecoverableWriter fileSystemWriter,
			final RecoverableWriter.ResumeRecoverable resumable,
			final long creationTime) throws IOException {
		Preconditions.checkNotNull(bucketId);
		Preconditions.checkNotNull(fileSystemWriter);
		Preconditions.checkNotNull(resumable);

		final RecoverableFsDataOutputStream stream = fileSystemWriter.recover(resumable);
		return new PartFileHandler<>(bucketId, stream, creationTime);
	}

	public static <IN> PartFileHandler<IN> openNew(
			final String bucketId,
			final RecoverableWriter fileSystemWriter,
			final Path path,
			final long creationTime) throws IOException {
		Preconditions.checkNotNull(bucketId);
		Preconditions.checkNotNull(fileSystemWriter);
		Preconditions.checkNotNull(path);

		final RecoverableFsDataOutputStream stream = fileSystemWriter.open(path);
		return new PartFileHandler<>(bucketId, stream, creationTime);
	}

	void write(IN element, Encoder<IN> encoder, long currentTime) throws IOException {
		encoder.encode(element, currentPartStream);
		this.lastUpdateTime = currentTime;
	}

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

	@Override
	public String getBucketId() {
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
}
