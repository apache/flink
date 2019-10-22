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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager.AllocationRequest;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.runtime.memory.MemoryManager.AllocationRequest.ofAllTypes;
import static org.apache.flink.runtime.memory.MemoryManager.AllocationRequest.ofType;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the memory manager.
 */
public class MemoryManagerTest {

	private static final long RANDOM_SEED = 643196033469871L;

	private static final int MEMORY_SIZE = 1024 * 1024 * 72; // 72 MiBytes

	private static final int PAGE_SIZE = 1024 * 32; // 32 KiBytes

	private static final int NUM_PAGES = MEMORY_SIZE / PAGE_SIZE;

	private MemoryManager memoryManager;

	private Random random;

	@Before
	public void setUp() {
		this.memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(MemoryType.HEAP, MEMORY_SIZE / 2)
			.setMemorySize(MemoryType.OFF_HEAP, MEMORY_SIZE / 2)
			.setPageSize(PAGE_SIZE)
			.build();
		this.random = new Random(RANDOM_SEED);
	}

	@After
	public void tearDown() {
		if (!this.memoryManager.verifyEmpty()) {
			fail("Memory manager is not complete empty and valid at the end of the test.");
		}
		this.memoryManager = null;
		this.random = null;
	}

	@Test
	public void allocateAllSingle() {
		try {
			final AbstractInvokable mockInvoke = new DummyInvokable();
			List<MemorySegment> segments = new ArrayList<MemorySegment>();

			try {
				for (int i = 0; i < NUM_PAGES; i++) {
					segments.add(this.memoryManager.allocatePages(mockInvoke, 1).get(0));
				}
			}
			catch (MemoryAllocationException e) {
				fail("Unable to allocate memory");
			}

			this.memoryManager.release(segments);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateAllMulti() {
		try {
			final AbstractInvokable mockInvoke = new DummyInvokable();
			final List<MemorySegment> segments = new ArrayList<MemorySegment>();

			try {
				for (int i = 0; i < NUM_PAGES / 2; i++) {
					segments.addAll(this.memoryManager.allocatePages(mockInvoke, 2));
				}
			} catch (MemoryAllocationException e) {
				Assert.fail("Unable to allocate memory");
			}

			this.memoryManager.release(segments);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateMultipleOwners() {
		final int numOwners = 17;

		try {
			AbstractInvokable[] owners = new AbstractInvokable[numOwners];

			@SuppressWarnings("unchecked")
			List<MemorySegment>[] mems = (List<MemorySegment>[]) new List<?>[numOwners];

			for (int i = 0; i < numOwners; i++) {
				owners[i] = new DummyInvokable();
				mems[i] = new ArrayList<MemorySegment>(64);
			}

			// allocate all memory to the different owners
			for (int i = 0; i < NUM_PAGES; i++) {
				final int owner = this.random.nextInt(numOwners);
				mems[owner].addAll(this.memoryManager.allocatePages(owners[owner], 1));
			}

			// free one owner at a time
			for (int i = 0; i < numOwners; i++) {
				this.memoryManager.releaseAll(owners[i]);
				owners[i] = null;
				Assert.assertTrue("Released memory segments have not been destroyed.", allMemorySegmentsFreed(mems[i]));
				mems[i] = null;

				// check that the owner owners were not affected
				for (int k = i + 1; k < numOwners; k++) {
					Assert.assertTrue("Non-released memory segments are accidentaly destroyed.", allMemorySegmentsValid(mems[k]));
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateTooMuch() {
		try {
			final AbstractInvokable mockInvoke = new DummyInvokable();

			List<MemorySegment> segs = this.memoryManager.allocatePages(mockInvoke, NUM_PAGES);

			testCannotAllocateAnymore(ofAllTypes(mockInvoke, 1));

			Assert.assertTrue("The previously allocated segments were not valid any more.",
																	allMemorySegmentsValid(segs));

			this.memoryManager.releaseAll(mockInvoke);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void doubleReleaseReturnsMemoryOnlyOnce() throws MemoryAllocationException {
		final AbstractInvokable mockInvoke = new DummyInvokable();

		Collection<MemorySegment> segs = this.memoryManager.allocatePages(ofAllTypes(mockInvoke, NUM_PAGES));
		MemorySegment segment = segs.iterator().next();

		this.memoryManager.release(segment);
		this.memoryManager.release(segment);

		testCannotAllocateAnymore(ofAllTypes(mockInvoke, 2));

		this.memoryManager.releaseAll(mockInvoke);
	}

	private boolean allMemorySegmentsValid(List<MemorySegment> memSegs) {
		for (MemorySegment seg : memSegs) {
			if (seg.isFreed()) {
				return false;
			}
		}
		return true;
	}

	private boolean allMemorySegmentsFreed(List<MemorySegment> memSegs) {
		for (MemorySegment seg : memSegs) {
			if (!seg.isFreed()) {
				return false;
			}
		}
		return true;
	}

	@Test
	@SuppressWarnings("NumericCastThatLosesPrecision")
	public void testAllocateMixedMemoryType() throws MemoryAllocationException {
		int totalHeapPages = (int) memoryManager.getMemorySizeByType(MemoryType.HEAP) / PAGE_SIZE;
		int totalOffHeapPages = (int) memoryManager.getMemorySizeByType(MemoryType.OFF_HEAP) / PAGE_SIZE;
		int pagesToAllocate =  totalHeapPages + totalOffHeapPages / 2;

		Object owner = new Object();
		Collection<MemorySegment> segments = memoryManager.allocatePages(ofAllTypes(owner, pagesToAllocate));
		Map<MemoryType, Integer> split = calcMemoryTypeSplitForSegments(segments);

		assertThat(split.get(MemoryType.HEAP), lessThanOrEqualTo(totalHeapPages));
		assertThat(split.get(MemoryType.OFF_HEAP), lessThanOrEqualTo(totalOffHeapPages));
		assertThat(split.get(MemoryType.HEAP) + split.get(MemoryType.OFF_HEAP), is(pagesToAllocate));

		memoryManager.release(segments);
	}

	private static Map<MemoryType, Integer> calcMemoryTypeSplitForSegments(Iterable<MemorySegment> segments) {
		int heapPages = 0;
		int offHeapPages = 0;
		for (MemorySegment memorySegment : segments) {
			if (memorySegment.isOffHeap()) {
				offHeapPages++;
			} else {
				heapPages++;
			}
		}
		Map<MemoryType, Integer> split = new EnumMap<>(MemoryType.class);
		split.put(MemoryType.HEAP, heapPages);
		split.put(MemoryType.OFF_HEAP, offHeapPages);
		return split;
	}

	@Test
	public void testMemoryReservation() throws MemoryReservationException {
		Object owner = new Object();

		memoryManager.reserveMemory(owner, MemoryType.HEAP, PAGE_SIZE);
		memoryManager.reserveMemory(owner, MemoryType.OFF_HEAP, memoryManager.getMemorySizeByType(MemoryType.OFF_HEAP));

		memoryManager.releaseMemory(owner, MemoryType.HEAP, PAGE_SIZE);
		memoryManager.releaseAllMemory(owner, MemoryType.OFF_HEAP);
	}

	@Test
	public void testCannotReserveBeyondTheLimit() throws MemoryReservationException {
		Object owner = new Object();
		memoryManager.reserveMemory(owner, MemoryType.OFF_HEAP, memoryManager.getMemorySizeByType(MemoryType.OFF_HEAP));
		testCannotReserveAnymore(MemoryType.OFF_HEAP, 1L);
		memoryManager.releaseAllMemory(owner, MemoryType.OFF_HEAP);
	}

	@Test
	public void testMemoryTooBigReservation() {
		long size = memoryManager.getMemorySizeByType(MemoryType.HEAP) + PAGE_SIZE;
		testCannotReserveAnymore(MemoryType.HEAP, size);
	}

	@Test
	public void testMemoryAllocationAndReservation() throws MemoryAllocationException, MemoryReservationException {
		MemoryType type = MemoryType.OFF_HEAP;
		@SuppressWarnings("NumericCastThatLosesPrecision")
		int totalPagesForType = (int) memoryManager.getMemorySizeByType(type) / PAGE_SIZE;

		// allocate half memory for segments
		Object owner1 = new Object();
		memoryManager.allocatePages(ofType(owner1, totalPagesForType / 2, MemoryType.OFF_HEAP));

		// reserve the other half of memory
		Object owner2 = new Object();
		memoryManager.reserveMemory(owner2, type, (long) PAGE_SIZE * totalPagesForType / 2);

		testCannotAllocateAnymore(ofType(new Object(), 1, type));
		testCannotReserveAnymore(type, 1L);

		memoryManager.releaseAll(owner1);
		memoryManager.releaseAllMemory(owner2, type);
	}

	private void testCannotAllocateAnymore(AllocationRequest request) {
		try {
			memoryManager.allocatePages(request);
			Assert.fail("Expected MemoryAllocationException. " +
				"We should not be able to allocate after allocating or(and) reserving all memory of a certain type.");
		} catch (MemoryAllocationException maex) {
			// expected
		}
	}

	private void testCannotReserveAnymore(MemoryType type, long size) {
		try {
			memoryManager.reserveMemory(new Object(), type, size);
			Assert.fail("Expected MemoryAllocationException. " +
				"We should not be able to any more memory after allocating or(and) reserving all memory of a certain type.");
		} catch (MemoryReservationException maex) {
			// expected
		}
	}
}
