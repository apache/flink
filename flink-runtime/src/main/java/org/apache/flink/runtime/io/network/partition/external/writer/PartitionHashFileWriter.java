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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A shuffle writer who writes a single file for each subpartition.
 */
public class PartitionHashFileWriter<T> implements PersistentFileWriter<T> {
	private static final Logger LOG = LoggerFactory.getLogger(PartitionHashFileWriter.class);

	private final int numPartitions;

	private final MemoryManager memoryManager;
	private final List<MemorySegment> memory;

	private final RecordSerializer<IOReadableWritable> recordSerializer;
	private final SerializationDelegate<T> serializationDelegate;

	private final FixedLengthBufferPool bufferPool;
	private final BufferFileWriter[] fileWriters;
	private final BufferBuilder[] currentBufferBuilders;
	private final long[] bytesWritten;

	private final Counter numBytesOut;
	private final Counter numBuffersOut;

	public PartitionHashFileWriter(
		int numPartitions,
		String partitionDataRootPath,
		MemoryManager memoryManager,
		List<MemorySegment> memory,
		IOManager ioManager,
		TypeSerializer<T> serializer) throws IOException {

		this(numPartitions, partitionDataRootPath, memoryManager, memory, ioManager, serializer, null, null);
	}

	public PartitionHashFileWriter(
		int numPartitions,
		String partitionDataRootPath,
		MemoryManager memoryManager,
		List<MemorySegment> memory,
		IOManager ioManager,
		TypeSerializer<T> serializer,
		Counter numBytesOut,
		Counter numBuffersOut) throws IOException {

		checkArgument(numPartitions > 0,
			"The number of subpartitions should be larger than 0, but actually is: " + numPartitions);
		this.numPartitions = numPartitions;

		// For each subpartition we need at least one segment for writing data, after the segment is full, it will be written
		// to file channel as a block in asynchronous way. In oder to make full usage of asynchronous performance, two
		// segments at least for each subpartition would be better.
		checkArgument(memory.size() >= numPartitions,
			"The number of memory segments should be more than that of subpartitions, but actually numMemory: "
				+ memory.size() + ", numPartitions: " + numPartitions);


		this.memoryManager = checkNotNull(memoryManager);
		this.memory = memory;

		this.recordSerializer = new SpanningRecordSerializer<>();
		this.serializationDelegate = new SerializationDelegate<>(serializer);

		this.bufferPool = new FixedLengthBufferPool(memory, false);
		this.fileWriters = new BufferFileWriter[numPartitions];
		this.currentBufferBuilders = new BufferBuilder[numPartitions];
		this.bytesWritten = new long[numPartitions];

		for (int i = 0; i < numPartitions; i++) {
			String path = ExternalBlockShuffleUtils.generateDataPath(partitionDataRootPath, i);
			fileWriters[i] = ioManager.createStreamFileWriter(ioManager.createChannel(new File(path)));
			bytesWritten[i] = 0;
		}

		this.numBytesOut = numBytesOut;
		this.numBuffersOut = numBuffersOut;
	}

	@Override
	public void add(T record, int targetPartition) throws IOException, InterruptedException {
		serializationDelegate.setInstance(record);
		recordSerializer.serializeRecord(serializationDelegate);
		copyToTargetFile(targetPartition);
	}

	@Override
	public void add(T record, int[] targetPartitions) throws IOException, InterruptedException {
		serializationDelegate.setInstance(record);

		recordSerializer.serializeRecord(serializationDelegate);

		for (int partition : targetPartitions) {
			copyToTargetFile(partition);
		}
	}

	@Override
	public void finish() throws IOException, InterruptedException {
		for (int i = 0; i < numPartitions; i++) {
			tryFinishCurrentBufferBuilder(i);
			fileWriters[i].close();
		}
	}

	@Override
	public List<List<PartitionIndex>> generatePartitionIndices() throws IOException, InterruptedException {
		List<PartitionIndex> partitionIndex = new ArrayList<>();

		for (int i = 0; i < numPartitions; ++i) {
			partitionIndex.add(new PartitionIndex(i, 0, bytesWritten[i]));
		}

		return Collections.singletonList(partitionIndex);
	}

	@Override
	public void clear() throws IOException {
		memoryManager.release(memory);
		bufferPool.lazyDestroy();
	}

	@Override
	public PersistentFileType getExternalFileType() {
		return PersistentFileType.HASH_PARTITION_FILE;
	}

	private void copyToTargetFile(int partition) throws IOException, InterruptedException {
		recordSerializer.reset();

		BufferBuilder bufferBuilder = getCurrentBufferBuilder(partition);
		RecordSerializer.SerializationResult result = recordSerializer.copyToBufferBuilder(bufferBuilder);

		while (result.isFullBuffer()) {
			tryFinishCurrentBufferBuilder(partition);

			if (result.isFullRecord()) {
				break;
			}

			bufferBuilder = getCurrentBufferBuilder(partition);
			result = recordSerializer.copyToBufferBuilder(bufferBuilder);
		}

		checkState(!recordSerializer.hasSerializedData(), "All data should be written at once");
	}

	private BufferBuilder getCurrentBufferBuilder(int partition) throws InterruptedException {
		if (currentBufferBuilders[partition] == null) {
			currentBufferBuilders[partition] = bufferPool.requestBufferBuilderBlocking();
			checkState(currentBufferBuilders[partition] != null,
				"Failed to request a buffer.");
		}

		return currentBufferBuilders[partition];
	}

	private void tryFinishCurrentBufferBuilder(int partition) throws IOException {
		if (currentBufferBuilders[partition] != null) {
			currentBufferBuilders[partition].finish();

			BufferConsumer consumer = currentBufferBuilders[partition].createBufferConsumer();
			Buffer buffer = consumer.build();

			bytesWritten[partition] += buffer.getSize();
			fileWriters[partition].writeBlock(buffer);

			if (numBytesOut != null) {
				numBytesOut.inc(buffer.getSize());
			}

			if (numBuffersOut != null) {
				numBuffersOut.inc();
			}

			consumer.close();
			currentBufferBuilders[partition] = null;
		}
	}
}
