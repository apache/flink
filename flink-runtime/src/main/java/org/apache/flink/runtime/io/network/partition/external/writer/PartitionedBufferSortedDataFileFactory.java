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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.operators.sort.SortedDataFileFactory;

import java.io.IOException;
import java.util.List;

/**
 * Factory to create BufferSortedDataFile.
 */
public class PartitionedBufferSortedDataFileFactory<T> implements SortedDataFileFactory<Tuple2<Integer, T>> {
	private final BufferSortedDataFileFactory<T> bufferSortedDataFileFactory;
	private final int numberPartitions;

	public PartitionedBufferSortedDataFileFactory(BufferSortedDataFileFactory<T> bufferSortedDataFileFactory, int numberPartitions) {
		this.bufferSortedDataFileFactory = bufferSortedDataFileFactory;
		this.numberPartitions = numberPartitions;
	}

	@Override
	public SortedDataFile<Tuple2<Integer, T>> createFile(List<MemorySegment> writeMemory) throws IOException {
		BufferSortedDataFile<T> bufferSortedDataFile = (BufferSortedDataFile<T>) bufferSortedDataFileFactory.createFile(writeMemory);
		return new PartitionedBufferSortedDataFile<>(numberPartitions, bufferSortedDataFile);
	}
}
