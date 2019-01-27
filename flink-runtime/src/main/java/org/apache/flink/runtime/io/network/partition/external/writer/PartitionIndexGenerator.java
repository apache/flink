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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;

import java.util.ArrayList;
import java.util.List;

/**
 * Maintains the statistics for current partition and generates the partition indices for a partitioned data file.
 */
public class PartitionIndexGenerator {
	private final int numberOfPartitions;
	private final List<PartitionIndex> partitionIndices;

	private long lastRecordsWritten = 0;

	private int currentPartition = 0;
	private long currentPartitionStartOffset = 0;

	public PartitionIndexGenerator(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
		this.partitionIndices = new ArrayList<>(numberOfPartitions);
	}

	public void updatePartitionIndexBeforeWriting(
		int partition, long numBytesWrittenBeforeWriting, long numRecordsWrittenBeforeWriting) {

		if (partition != currentPartition) {
			PartitionIndex partitionIndex = new PartitionIndex(currentPartition, currentPartitionStartOffset,
				numBytesWrittenBeforeWriting - currentPartitionStartOffset,
				numRecordsWrittenBeforeWriting - lastRecordsWritten);
			partitionIndices.add(partitionIndex);

			for (int i = currentPartition + 1; i < partition; ++i) {
				partitionIndices.add(new PartitionIndex(i, numBytesWrittenBeforeWriting, 0, 0));
			}

			lastRecordsWritten = numRecordsWrittenBeforeWriting;

			currentPartition = partition;
			currentPartitionStartOffset = numBytesWrittenBeforeWriting;
		}
	}

	public void finishWriting(long numBytesWrittenBefore, long numRecordsWrittenBefore) {
		PartitionIndex partitionIndex = new PartitionIndex(currentPartition, currentPartitionStartOffset,
			numBytesWrittenBefore - currentPartitionStartOffset, numRecordsWrittenBefore - lastRecordsWritten);
		partitionIndices.add(partitionIndex);

		for (int i = currentPartition + 1; i < numberOfPartitions; ++i) {
			partitionIndices.add(new PartitionIndex(i, numBytesWrittenBefore, 0, 0));
		}
	}

	public List<PartitionIndex> getPartitionIndices() {
		return partitionIndices;
	}
}
