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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A state handle used in {@link FsSegmentCheckpointStreamFactory}.
 */
public class FsSegmentStateHandle implements StreamStateHandle {
	private static final long serialVersionUID = 26L;

	/** The file where the segment locates. */
	private final Path filePath;

	/** The start position(inclusive) of the snapshot data in the file. */
	private final long startPosition;

	/** The end position(exclusive) of the snapshot data in the file. */
	private final long endPosition;

	/** The state size of this state handle. */
	private final long stateSize;

	public FsSegmentStateHandle(
		final Path filePath,
		final long startPosition,
		final long endPosition) {
		checkArgument(filePath != null);
		checkArgument(startPosition >= 0 && endPosition > startPosition);
		this.filePath = checkNotNull(filePath);
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.stateSize = endPosition - startPosition;
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		FileSystem fileSystem = FileSystem.get(filePath.toUri());
		FSDataInputStream inputStream = fileSystem.open(filePath);
		inputStream.seek(startPosition);
		return inputStream;
	}

	@Override
	public void discardState() throws Exception {
		// avoid to delete the underlying file, it will be deleted by JM.
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	public Path getFilePath() {
		return filePath;
	}

	public long getStartPosition() {
		return startPosition;
	}

	public long getEndPosition() {
		return endPosition;
	}

	@Override
	public String toString() {
		return "FileSegmentStateHandle{" +
			"filePath=" + filePath +
			", startPosition=" + startPosition +
			", endPosition=" + endPosition +
			", stateSize=" + getStateSize() +
			"}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof FsSegmentStateHandle)) {
			return false;
		}

		FsSegmentStateHandle that = (FsSegmentStateHandle) o;
		return filePath.equals(that.filePath) &&
			startPosition == that.startPosition &&
			endPosition == that.endPosition;
	}

	@Override
	public int hashCode() {
		int result = filePath.hashCode();
		result = 31 * result + (int) (startPosition ^ (startPosition >>> 32));
		result = 31 * result + (int) (endPosition ^ (endPosition >>> 32));
		return result;
	}
}

