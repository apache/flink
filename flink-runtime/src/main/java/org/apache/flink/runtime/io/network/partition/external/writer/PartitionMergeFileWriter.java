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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DuplicateOnlySerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.SerializerManager;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.PushedUnilateralSortMerger;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.operators.sort.SortedDataFileMerger;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Shuffle writing using the outer sort-merger.
 */
public class PartitionMergeFileWriter<T> implements PersistentFileWriter<T> {
	private static final Logger LOG = LoggerFactory.getLogger(PartitionMergeFileWriter.class);

	private final String partitionDataRootPath;

	private final TypeSerializer<T> typeSerializer;

	private final List<MemorySegment> allMemory;

	private final Tuple2<Integer, T> reuse = new Tuple2<>();

	private final PushedUnilateralSortMerger<Tuple2<Integer, T>> sortMerger;

	public PartitionMergeFileWriter(
		int numPartitions,
		String partitionDataRootPath,
		int mergeFactor,
		boolean enableAsyncMerging,
		boolean mergeToOneFile,
		MemoryManager memoryManager,
		List<MemorySegment> memory,
		IOManager ioManager,
		TypeSerializer<T> serializer,
		SerializerManager<SerializationDelegate<T>> serializerManager,
		AbstractInvokable parentTask) throws IOException, MemoryAllocationException {

		this(numPartitions, partitionDataRootPath, mergeFactor, enableAsyncMerging, mergeToOneFile,
			memoryManager, memory, ioManager, serializer, serializerManager, parentTask, null, null);
	}

	public PartitionMergeFileWriter(
		int numPartitions,
		String partitionDataRootPath,
		int mergeFactor,
		boolean enableAsyncMerging,
		boolean mergeToOneFile,
		MemoryManager memoryManager,
		List<MemorySegment> memory,
		IOManager ioManager,
		TypeSerializer<T> serializer,
		SerializerManager<SerializationDelegate<T>> serializerManager,
		AbstractInvokable parentTask,
		Counter numBytesOut,
		Counter numBuffersOut) throws IOException, MemoryAllocationException {
		checkArgument(numPartitions > 0,
			"The number of subpartitions should be larger than 0, but actually is: " + numPartitions);
		checkArgument(mergeFactor >= 2, "Illegal merge factor: " + mergeFactor);

		this.partitionDataRootPath = partitionDataRootPath;

		this.typeSerializer = serializer;

		this.allMemory = memory;

		// Create the sort merger
		Class<Tuple2<Integer, T>> typedTuple = (Class<Tuple2<Integer, T>>) (Class<?>) Tuple2.class;
		TypeSerializer<?>[] serializers = new TypeSerializer[]{IntSerializer.INSTANCE, serializer.duplicate()};
		TypeSerializer<Tuple2<Integer, T>> tuple2Serializer = new TupleSerializer<>(typedTuple, serializers);
		DuplicateOnlySerializerFactory<Tuple2<Integer, T>> serializerFactory = new DuplicateOnlySerializerFactory<>(tuple2Serializer);

		int[] keyPositions = new int[]{0};
		TypeComparator<?>[] comparators = new TypeComparator<?>[]{new IntComparator(true)};
		TupleComparator<Tuple2<Integer, T>> tuple2Comparator = new TupleComparator<>(
			keyPositions, comparators, serializers);

		BufferSortedDataFileFactory<T> sortedDataFileFactory = new BufferSortedDataFileFactory<>(
			partitionDataRootPath, typeSerializer, ioManager, serializerManager, numBytesOut, numBuffersOut);

		PartitionedBufferSortedDataFileFactory<T> partitionedBufferSortedDataFileFactory =
			new PartitionedBufferSortedDataFileFactory<T>(sortedDataFileFactory, numPartitions);

		SortedDataFileMerger<Tuple2<Integer, T>> merger = new ConcatPartitionedFileMerger<T>(
			numPartitions, partitionDataRootPath, mergeFactor, enableAsyncMerging, mergeToOneFile, ioManager);

		sortMerger = new PushedUnilateralSortMerger<>(partitionedBufferSortedDataFileFactory, merger,
			memoryManager, allMemory, ioManager, parentTask, serializerFactory, tuple2Comparator,
			0, mergeFactor, false, 0,
			false, true, true, enableAsyncMerging);

		LOG.info("External result partition writer initialized.");
	}

	@Override
	public void add(T record, int targetPartition) throws IOException {
		reuse.f1 = record;
		reuse.f0 = targetPartition;
		sortMerger.add(reuse);
	}

	@Override
	public void add(T record, int[] targetPartitions) throws IOException {
		reuse.f1 = record;
		for (int partition : targetPartitions) {
			reuse.f0 = partition;
			sortMerger.add(reuse);
		}
	}

	@Override
	public void finish() throws IOException, InterruptedException {
		sortMerger.finishAdding();

		List<SortedDataFile<Tuple2<Integer, T>>> remainFiles = sortMerger.getRemainingSortedDataFiles();

		int nextFileId = 0;
		FileSystem localFileSystem = FileSystem.getLocalFileSystem();
		for (SortedDataFile<Tuple2<Integer, T>> file : remainFiles) {
			// rename file
			localFileSystem.rename(
				new Path(file.getChannelID().getPath()),
				new Path(ExternalBlockShuffleUtils.generateDataPath(partitionDataRootPath, nextFileId++)));
		}

		LOG.info("Finish external result partition writing.");
	}

	@Override
	public List<List<PartitionIndex>> generatePartitionIndices() throws IOException, InterruptedException {
		List<List<PartitionIndex>> partitionIndices = new ArrayList<>();
		List<SortedDataFile<Tuple2<Integer, T>>> remainFiles = sortMerger.getRemainingSortedDataFiles();

		for (SortedDataFile<Tuple2<Integer, T>> file : remainFiles) {
			if (!(file instanceof PartitionedSortedDataFile)) {
				throw new IllegalStateException("Unexpected file type.");
			}

			partitionIndices.add(((PartitionedSortedDataFile<T>) file).getPartitionIndexList());
		}

		return partitionIndices;
	}

	@Override
	public void clear() throws IOException {
		// nothing to do
	}

	@Override
	public PersistentFileType getExternalFileType() {
		return PersistentFileType.MERGED_PARTITION_FILE;
	}
}
