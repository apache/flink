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

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.BUCKET_SIZE;

/**
 * Tests for {@link SpaceAllocator}.
 */
public class SpaceAllocatorTest extends TestLogger {

	@Test
	public void testConstruct() throws Exception {
		int chunkSize = 64 * 1024 * 1024;

		// construct heap space allocator with preallocate
		SpaceConfiguration spaceConfiguration1 =
			new SpaceConfiguration(chunkSize, true, ChunkAllocator.SpaceType.HEAP);
		SpaceAllocator spaceAllocator1 = new SpaceAllocator(spaceConfiguration1);
		Assert.assertTrue(spaceAllocator1.getChunkAllocator() instanceof HeapBufferChunkAllocator);
		Assert.assertEquals(1, spaceAllocator1.getChunkIdGenerator().get());
		spaceAllocator1.close();

		// construct off-heap space allocator without preallocate
		SpaceConfiguration spaceConfiguration2 =
			new SpaceConfiguration(chunkSize, false, ChunkAllocator.SpaceType.OFFHEAP);
		SpaceAllocator spaceAllocator2 = new SpaceAllocator(spaceConfiguration2);
		Assert.assertTrue(spaceAllocator2.getChunkAllocator() instanceof DirectBufferChunkAllocator);
		Assert.assertEquals(0, spaceAllocator2.getChunkIdGenerator().get());
		spaceAllocator2.allocate(12);
		spaceAllocator2.close();
	}

	@Test
	public void testNormal() throws Exception {
		int chunkSize = 64 * 1024 * 1024;
		SpaceConfiguration spaceConfiguration =
			new SpaceConfiguration(chunkSize, false, ChunkAllocator.SpaceType.HEAP);
		TestChunkAllocator chunkAllocator = new TestChunkAllocator(spaceConfiguration);
		SpaceAllocator spaceAllocator = new SpaceAllocator(spaceConfiguration, chunkAllocator);

		// allocate normal space
		long offset0 = spaceAllocator.allocate(1);
		Assert.assertEquals(0, SpaceUtils.getChunkIdByAddress(offset0));
		Assert.assertEquals(0, SpaceUtils.getChunkOffsetByAddress(offset0));
		TestChunk chunk0 = (TestChunk) (spaceAllocator.getChunkById(0));
		Assert.assertEquals(0, chunk0.getChunkId());
		Assert.assertEquals(chunkAllocator.chunkSize, chunk0.getChunkCapacity());
		Assert.assertEquals(1, chunk0.offset);
		Assert.assertEquals(1, chunk0.usedSpace.get(0).intValue());

		// allocate normal space
		long offset1 = spaceAllocator.allocate(1234);
		Assert.assertEquals(0, SpaceUtils.getChunkIdByAddress(offset1));
		Assert.assertEquals(1, SpaceUtils.getChunkOffsetByAddress(offset1));
		Assert.assertEquals(1235, chunk0.offset);
		Assert.assertEquals(1, chunk0.usedSpace.get(0).intValue());
		Assert.assertEquals(1234, chunk0.usedSpace.get(1).intValue());

		// allocate huge space
		long offset2 = spaceAllocator.allocate(BUCKET_SIZE);
		Assert.assertEquals(1, SpaceUtils.getChunkIdByAddress(offset2));
		Assert.assertEquals(0, SpaceUtils.getChunkOffsetByAddress(offset2));

		TestChunk chunk1 = (TestChunk) (spaceAllocator.getChunkById(1));
		Assert.assertEquals(1, chunk1.getChunkId());
		Assert.assertEquals(BUCKET_SIZE, chunk1.offset);
		Assert.assertEquals(BUCKET_SIZE, chunk1.usedSpace.get(0).intValue());

		// allocate normal space, and chunk 0 will be used
		long offset3 = spaceAllocator.allocate(12345);
		Assert.assertEquals(0, SpaceUtils.getChunkIdByAddress(offset3));
		Assert.assertEquals(1235, SpaceUtils.getChunkOffsetByAddress(offset3));
		Assert.assertEquals(13580, chunk0.offset);
		Assert.assertEquals(1, chunk0.usedSpace.get(0).intValue());
		Assert.assertEquals(1234, chunk0.usedSpace.get(1).intValue());
		Assert.assertEquals(12345, chunk0.usedSpace.get(1235).intValue());

		// allocate normal space, and a new chunk will be created
		chunk0.offset = chunkAllocator.chunkSize - 1;
		long offset4 = spaceAllocator.allocate(3);
		Assert.assertEquals(2, SpaceUtils.getChunkIdByAddress(offset4));
		Assert.assertEquals(0, SpaceUtils.getChunkOffsetByAddress(offset4));
		TestChunk chunk2 = (TestChunk) (spaceAllocator.getChunkById(2));
		Assert.assertEquals(2, chunk2.getChunkId());
		Assert.assertEquals(3, chunk2.usedSpace.get(0).intValue());

		// free space
		Assert.assertEquals(3, chunk0.usedSpace.size());
		Assert.assertEquals(1, chunk1.usedSpace.size());
		Assert.assertEquals(1, chunk2.usedSpace.size());

		spaceAllocator.free(offset4);
		Assert.assertEquals(3, chunk0.usedSpace.size());
		Assert.assertEquals(1, chunk1.usedSpace.size());
		Assert.assertEquals(0, chunk2.usedSpace.size());

		spaceAllocator.free(offset3);
		Assert.assertEquals(2, chunk0.usedSpace.size());
		Assert.assertEquals(1, chunk1.usedSpace.size());
		Assert.assertFalse(chunk0.usedSpace.containsKey(SpaceUtils.getChunkOffsetByAddress(offset3)));

		spaceAllocator.free(offset2);
		Assert.assertEquals(2, chunk0.usedSpace.size());
		Assert.assertEquals(0, chunk1.usedSpace.size());

		spaceAllocator.free(offset1);
		Assert.assertEquals(1, chunk0.usedSpace.size());
		Assert.assertFalse(chunk0.usedSpace.containsKey(SpaceUtils.getChunkOffsetByAddress(offset1)));

		spaceAllocator.free(offset0);
		Assert.assertEquals(0, chunk0.usedSpace.size());
		Assert.assertFalse(chunk0.usedSpace.containsKey(SpaceUtils.getChunkOffsetByAddress(offset0)));

		spaceAllocator.close();
	}

	@Test
	public void testExpand() throws Exception {
		int chunkSize = 1024;
		SpaceConfiguration spaceConfiguration =
			new SpaceConfiguration(chunkSize, false, ChunkAllocator.SpaceType.HEAP);
		TestChunkAllocator chunkAllocator = new TestChunkAllocator(spaceConfiguration);
		SpaceAllocator spaceAllocator = new SpaceAllocator(spaceConfiguration, chunkAllocator);
		spaceAllocator.addTotalSpace(new TestChunk(0, chunkSize), 0);
		spaceAllocator.addTotalSpace(new TestChunk(16, chunkSize), 16);
		Assert.assertEquals(0, spaceAllocator.getChunkById(0).getChunkId());
		Assert.assertEquals(16, spaceAllocator.getChunkById(16).getChunkId());

		spaceAllocator.close();
	}

	@Test
	public void testClose() throws Exception {
		int chunkSize = 1024;
		SpaceConfiguration spaceConfiguration =
			new SpaceConfiguration(chunkSize, false, ChunkAllocator.SpaceType.HEAP);
		TestChunkAllocator chunkAllocator = new TestChunkAllocator(spaceConfiguration);
		SpaceAllocator spaceAllocator = new SpaceAllocator(spaceConfiguration, chunkAllocator);
		Assert.assertFalse(chunkAllocator.closed);

		spaceAllocator.close();
		Assert.assertTrue(chunkAllocator.closed);
	}

	private class TestChunkAllocator implements ChunkAllocator {

		int chunkSize;
		boolean closed;

		TestChunkAllocator(SpaceConfiguration spaceConfiguration) {
			this.chunkSize = spaceConfiguration.getChunkSize();
			this.closed = false;
		}

		@Override
		public Chunk createChunk(int chunkId, AllocateStrategy allocateStrategy) {
			return new TestChunk(chunkId, chunkSize);
		}

		@Override
		public void close() {
			closed = true;
		}
	}

	private class TestChunk extends AbstractChunk {

		int offset = 0;
		Map<Integer, Integer> usedSpace = new HashMap<>();

		TestChunk(int chunkId, int capacity) {
			super(chunkId, capacity);
		}

		@Override
		public int allocate(int len) {
			if (this.offset + len >= capacity) {
				return -1;
			}
			int curOffset = this.offset;
			this.offset += len;
			usedSpace.put(curOffset, len);
			return curOffset;
		}

		@Override
		public void free(int offset) {
			Preconditions.checkArgument(usedSpace.containsKey(offset));
			usedSpace.remove(offset);

		}

		@Override
		public ByteBuffer getByteBuffer(int chunkOffset) {
			return null;
		}

		@Override
		public int getOffsetInByteBuffer(int offsetInChunk) {
			return 0;
		}
	}
}
