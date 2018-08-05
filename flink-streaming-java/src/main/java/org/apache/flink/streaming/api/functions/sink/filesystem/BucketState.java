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
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * The state of the {@link Bucket} that is to be checkpointed.
 */
@Internal
public class BucketState<BucketID> {

	private final BucketID bucketId;

	/**
	 * The base path for the bucket, i.e. the directory where all the part files are stored.
	 */
	private final Path bucketPath;

	/**
	 * The creation time of the currently open part file, or {@code Long.MAX_VALUE} if there is no open part file.
	 */
	private final long creationTime;

	/**
	 * A {@link RecoverableWriter.ResumeRecoverable} for the currently open part file, or null
	 * if there is no currently open part file.
	 */
	@Nullable
	private final RecoverableWriter.ResumeRecoverable inProgress;

	/**
	 * The {@link RecoverableWriter.CommitRecoverable files} pending to be committed, organized by checkpoint id.
	 */
	private final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingPerCheckpoint;

	public BucketState(
			final BucketID bucketId,
			final Path bucketPath,
			final long creationTime,
			@Nullable final RecoverableWriter.ResumeRecoverable inProgress,
			final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingPerCheckpoint
	) {
		this.bucketId = Preconditions.checkNotNull(bucketId);
		this.bucketPath = Preconditions.checkNotNull(bucketPath);
		this.creationTime = creationTime;
		this.inProgress = inProgress;
		this.pendingPerCheckpoint = Preconditions.checkNotNull(pendingPerCheckpoint);
	}

	public BucketID getBucketId() {
		return bucketId;
	}

	public Path getBucketPath() {
		return bucketPath;
	}

	public long getCreationTime() {
		return creationTime;
	}

	@Nullable
	public RecoverableWriter.ResumeRecoverable getInProgress() {
		return inProgress;
	}

	public Map<Long, List<RecoverableWriter.CommitRecoverable>> getPendingPerCheckpoint() {
		return pendingPerCheckpoint;
	}
}
