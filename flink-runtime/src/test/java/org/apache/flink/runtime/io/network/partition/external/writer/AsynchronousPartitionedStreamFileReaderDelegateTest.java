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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AsynchronousPartitionedStreamFileReaderDelegateTest {
	private static final int MEMORY_SEGMENT_SIZE = 1024;

	private IOManager ioManager;

	@Before
	public void before() {
		this.ioManager = new IOManagerAsync();
	}

	@Test
	public void testRead() throws IOException, InterruptedException {
		FileIOChannel.ID channel = ioManager.createChannel();
		BufferFileWriter writer = ioManager.createStreamFileWriter(channel);

		List<PartitionIndex> partitionIndices = new ArrayList<>(5);
		partitionIndices.add(new PartitionIndex(0, 0, MEMORY_SEGMENT_SIZE * 2 + 10));
		partitionIndices.add(new PartitionIndex(1, MEMORY_SEGMENT_SIZE * 2 + 10, MEMORY_SEGMENT_SIZE * 3 + 11));
		partitionIndices.add(new PartitionIndex(2, MEMORY_SEGMENT_SIZE * 5 + 21, 0));
		partitionIndices.add(new PartitionIndex(3, MEMORY_SEGMENT_SIZE * 5 + 21, MEMORY_SEGMENT_SIZE * 2));
		partitionIndices.add(new PartitionIndex(4, MEMORY_SEGMENT_SIZE * 7 + 21, MEMORY_SEGMENT_SIZE + 33));
		partitionIndices.add(new PartitionIndex(5, MEMORY_SEGMENT_SIZE * 8 + 54, 0));
		partitionIndices.add(new PartitionIndex(6, MEMORY_SEGMENT_SIZE * 8 + 54, 0));

		final int expectedTotalBuffers = 11;
		final Map<Integer, Long> unfulfilledBuffers = new HashMap<Integer, Long>() {{
			put(2, 10L);
			put(6, 11L);
			put(10, 33L);
		}};

		for (int i = 0; i < expectedTotalBuffers; ++i) {
			Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(MEMORY_SEGMENT_SIZE), new BufferRecycler() {
				@Override
				public void recycle(MemorySegment memorySegment) {
					// NOP
				}
			});

			byte val = (byte) i;
			long bufferSize = unfulfilledBuffers.get(i) != null ? unfulfilledBuffers.get(i) : MEMORY_SEGMENT_SIZE;
			for (int j = 0; j < bufferSize; ++j) {
				buffer.getMemorySegment().put(j, val);
			}
			buffer.setSize((int) bufferSize);

			writer.writeBlock(buffer);
		}
		writer.close();

		// now read it
		List<MemorySegment> memory = new ArrayList<>();
		for (int i = 0; i < 3; ++i) {
			memory.add(MemorySegmentFactory.allocateUnpooledSegment(MEMORY_SEGMENT_SIZE));
		}

		AsynchronousPartitionedStreamFileReaderDelegate reader = new AsynchronousPartitionedStreamFileReaderDelegate(
			ioManager, channel, memory, partitionIndices);

		for (int i = 0; i < expectedTotalBuffers; ++i) {
			Buffer buffer = reader.getNextBufferBlocking();
			byte expectedVal = (byte) i;
			long expectedBufferSize = unfulfilledBuffers.get(i) != null ? unfulfilledBuffers.get(i) : MEMORY_SEGMENT_SIZE;
			Assert.assertEquals(expectedBufferSize, buffer.getSize());
			for (int j = 0; j < expectedBufferSize; ++j) {
				byte val = buffer.getMemorySegment().get(j);
				Assert.assertEquals(expectedVal, val);
			}
			buffer.recycleBuffer();
		}
	}

	@After
	public void after() {
		this.ioManager.shutdown();
	}
}
