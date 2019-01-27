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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;

/**
 * Tests the merge file writer.
 */
public class PartitionMergeFileWriterTest extends PersistentFileWriterTestBase {
	@Override
	protected PersistentFileWriter<Integer> createFileWriter(int numberPartitions, String partitionRootPath) throws Exception {

		Configuration configuration = new Configuration();

		return new PartitionMergeFileWriter<>(
			numberPartitions,
			partitionRootPath,
			2,
			false,
			false,
			memoryManager,
			memoryManager.allocatePages(parentTask, NUM_PAGES),
			ioManager,
			serializer,
			new SerializerManager<>(ResultPartitionType.BLOCKING, configuration),
			parentTask);
	}

	@Override
	protected MutableObjectIterator<Integer> createResultIterator(int numPartitions, String partitionRootPath, List<List<PartitionIndex>> partitionIndices, int subpartitionIndex) throws Exception {
		return new PartitionMergeFileRecordIterator(partitionRootPath, partitionIndices, subpartitionIndex);
	}

	private class PartitionMergeFileRecordIterator implements MutableObjectIterator<Integer> {
		private final String partitionRootPath;
		private final List<List<PartitionIndex>> partitionIndices;
		private final int subpartitionIndex;

		PartitionMergeFileRecordIterator(String partitionRootPath, List<List<PartitionIndex>> partitionIndices, int subpartitionIndex) {
			this.partitionRootPath = partitionRootPath;
			this.partitionIndices = partitionIndices;
			this.subpartitionIndex = subpartitionIndex;
		}

		private int currentDataFileIndex;
		private BufferSortedDataFileReader<Integer> currentReader;

		@Override
		public Integer next(Integer reuse) throws IOException {
			return next();
		}

		@Override
		public Integer next() throws IOException {
			while (true) {
				if (currentReader == null) {
					if (currentDataFileIndex >= partitionIndices.size()) {
						return null;
					}

					String dataFilePath = ExternalBlockShuffleUtils.generateDataPath(partitionRootPath, currentDataFileIndex);
					PartitionIndex partitionIndex = partitionIndices.get(currentDataFileIndex).get(subpartitionIndex);
					currentReader = new BufferSortedDataFileReader<Integer>(
						dataFilePath,
						temporaryFolder.newFolder().getAbsolutePath(),
						ioManager,
						PAGE_SIZE,
						serializer,
						partitionIndex.getStartOffset(),
						partitionIndex.getLength());
				}

				Integer result = currentReader.next();
				if (result != null) {
					return result;
				}

				currentDataFileIndex++;
				currentReader = null;
			}
		}
	}
}
