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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;

/**
 * States for {@link FileWriterBucket}.
 */
@Internal
public class FileWriterBucketState {

	private final String bucketId;

	/** The directory where all the part files of the bucket are stored. */
	private final Path bucketPath;

	/**
	 * The creation time of the currently open part file,
	 * or {@code Long.MAX_VALUE} if there is no open part file.
	 */
	private final long inProgressFileCreationTime;

	/**
	 * A {@link InProgressFileRecoverable} for the currently open
	 * part file, or null if there is no currently open part file.
	 */
	@Nullable
	private final InProgressFileRecoverable inProgressFileRecoverable;

	public FileWriterBucketState(
			String bucketId,
			Path bucketPath,
			long inProgressFileCreationTime,
			@Nullable InProgressFileRecoverable inProgressFileRecoverable) {
		this.bucketId = bucketId;
		this.bucketPath = bucketPath;
		this.inProgressFileCreationTime = inProgressFileCreationTime;
		this.inProgressFileRecoverable = inProgressFileRecoverable;
	}

	public String getBucketId() {
		return bucketId;
	}

	public Path getBucketPath() {
		return bucketPath;
	}

	public long getInProgressFileCreationTime() {
		return inProgressFileCreationTime;
	}

	@Nullable
	InProgressFileRecoverable getInProgressFileRecoverable() {
		return inProgressFileRecoverable;
	}

	boolean hasInProgressFileRecoverable() {
		return inProgressFileRecoverable != null;
	}

	@Override
	public String toString() {
		final StringBuilder strBuilder = new StringBuilder();

		strBuilder
				.append("BucketState for bucketId=").append(bucketId)
				.append(" and bucketPath=").append(bucketPath);

		if (hasInProgressFileRecoverable()) {
			strBuilder.append(", has open part file created @ ").append(inProgressFileCreationTime);
		}
		return strBuilder.toString();
	}
}
