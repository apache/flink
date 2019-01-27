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

import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * Tests the hash file writer.
 */
public class PartitionHashFileWriterTest extends PersistentFileWriterTestBase {

	@Override
	protected PersistentFileWriter<Integer> createFileWriter(int numberPartitions, String partitionRootPath) throws Exception {
		return new PartitionHashFileWriter<>(
			numberPartitions, partitionRootPath, memoryManager, memoryManager.allocatePages(this, NUM_PAGES),
			ioManager, serializer);
	}

	@Override
	protected MutableObjectIterator<Integer> createResultIterator(final int numPartitions,
																	final String partitionRootPath,
																	final List<List<PartitionIndex>> partitionIndices,
																	final int subpartitionIndex) throws Exception {
		assertEquals(1, partitionIndices.size());
		assertEquals(numPartitions, partitionIndices.get(0).size());

		PartitionIndex partitionIndex = partitionIndices.get(0).get(subpartitionIndex);
		String dataFilePath = ExternalBlockShuffleUtils.generateDataPath(partitionRootPath, subpartitionIndex);

		assertEquals(0, partitionIndex.getStartOffset());

		final BufferSortedDataFileReader<Integer> fileReader = new BufferSortedDataFileReader<>(
			dataFilePath,
			temporaryFolder.newFolder().getAbsolutePath(),
			ioManager,
			PAGE_SIZE,
			serializer,
			partitionIndex.getStartOffset(),
			partitionIndex.getLength());

		return new MutableObjectIterator<Integer>() {
			@Override
			public Integer next(Integer reuse) throws IOException {
				return reuse;
			}

			@Override
			public Integer next() throws IOException {
				return fileReader.next();
			}
		};
	}
}
