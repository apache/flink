/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

/**
 * Partition index for external data file.
 */
public class PartitionIndex {
	/** The subpartition index. */
	private final int partition;

	/** The offset in the file. */
	private final long startOffset;

	/** The length of data. */
	private final long length;

	/** The number of records of this part. */
	private final transient long numRecords;

	public PartitionIndex(int partition, long startOffset, long length) {
		this(partition, startOffset, length, 0);
	}

	public PartitionIndex(int partition, long startOffset, long length, long numRecords) {
		this.partition = partition;
		this.startOffset = startOffset;
		this.length = length;
		this.numRecords = numRecords;
	}

	public int getPartition() {
		return partition;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public long getLength() {
		return length;
	}

	public long getNumRecords() {
		return numRecords;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		PartitionIndex that = (PartitionIndex) o;

		return partition == that.partition
			&& startOffset == that.startOffset
			&& length == that.length
			&& numRecords == that.numRecords;
	}

	@Override
	public int hashCode() {
		int result = partition;
		result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
		result = 31 * result + (int) (length ^ (length >>> 32));
		result = 31 * result + (int) (numRecords ^ (numRecords >>> 32));
		return result;
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("PartitionIndex :{ partition=").append(partition)
			.append(", startOffset=").append(startOffset)
			.append(", length=").append(length)
			.append(", numRecords=").append(numRecords)
			.append("}");
		return stringBuilder.toString();
	}
}
