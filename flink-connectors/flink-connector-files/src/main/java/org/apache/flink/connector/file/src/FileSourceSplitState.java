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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * State of the reader, essentially a mutable version of the {@link FileSourceSplit}.
 * Has a modifiable offset and records-to-skip-count.
 *
 * <p>The {@link FileSourceSplit} assigned to the reader or stored in the checkpoint points to the
 * position from where to start reading (after recovery), so the current offset and records-to-skip
 * need to always point to the record after the last emitted record.
 */
@PublicEvolving
public final class FileSourceSplitState {

	private final FileSourceSplit split;

	private long offset;

	private long recordsToSkipAfterOffset;

	public FileSourceSplitState(FileSourceSplit split) {
		this.split = checkNotNull(split);

		final Optional<CheckpointedPosition> readerPosition = split.getReaderPosition();
		if (readerPosition.isPresent()) {
			this.offset = readerPosition.get().getOffset();
			this.recordsToSkipAfterOffset = readerPosition.get().getRecordsAfterOffset();
		} else {
			this.offset = CheckpointedPosition.NO_OFFSET;
			this.recordsToSkipAfterOffset = 0L;
		}
	}

	public long getOffset() {
		return offset;
	}

	public long getRecordsToSkipAfterOffset() {
		return recordsToSkipAfterOffset;
	}

	public void setOffset(long offset) {
		// we skip sanity / boundary checks here for efficiency.
		// illegal boundaries will eventually be caught when constructing the split on checkpoint.
		this.offset = offset;
	}

	public void setRecordsToSkipAfterOffset(long recordsToSkipAfterOffset) {
		// we skip sanity / boundary checks here for efficiency.
		// illegal boundaries will eventually be caught when constructing the split on checkpoint.
		this.recordsToSkipAfterOffset = recordsToSkipAfterOffset;
	}

	public void setPosition(long offset, long recordsToSkipAfterOffset) {
		// we skip sanity / boundary checks here for efficiency.
		// illegal boundaries will eventually be caught when constructing the split on checkpoint.
		this.offset = offset;
		this.recordsToSkipAfterOffset = recordsToSkipAfterOffset;
	}

	public void setPosition(CheckpointedPosition position) {
		this.offset = position.getOffset();
		this.recordsToSkipAfterOffset = position.getRecordsAfterOffset();
	}

	/**
	 * Use the current row count as the starting row count to create a new FileSourceSplit.
	 */
	public FileSourceSplit toFileSourceSplit() {
		final CheckpointedPosition position =
				(offset == CheckpointedPosition.NO_OFFSET && recordsToSkipAfterOffset == 0) ?
						null : new CheckpointedPosition(offset, recordsToSkipAfterOffset);

		return new FileSourceSplit(
				split.splitId(),
				split.path(),
				split.offset(),
				split.length(),
				split.hostnames(),
				position);
	}
}
