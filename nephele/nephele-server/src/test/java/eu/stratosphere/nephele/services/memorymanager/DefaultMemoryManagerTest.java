/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.memorymanager;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class DefaultMemoryManagerTest
{
	public static final long RANDOM_SEED = 643196033469871L;

	public static final int MEMORY_SIZE = 1024 * 1024 * 72; // 72 MB

	public static final int CHUNK_SIZE = 1024 * 1024 * 12; // 12 MB

	public static final int[] SEGMENT_SIZES = { 1024 * 1024 * 12, 1024 * 1024 * 6, 1024 * 1024 * 4 };

	private DefaultMemoryManager memoryManager;

	private Random random;

	@Before
	public void setUp() {
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE, CHUNK_SIZE);
		random = new Random(RANDOM_SEED);
	}

	@After
	public void tearDown() throws Exception {
		if (!memoryManager.verifyEmpty()) {
			Assert.fail("Memory manager is not complete empty and valid at the end of the test.");
		}
		
		memoryManager = null;
		random = null;
	}

	@Test
	public void allocateAllSingle() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		
		int numSplits = 6;
		if(MEMORY_SIZE%numSplits != 0) throw new Exception("Invalid number of splits");
		
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		
		try {
			for(int i=0;i<numSplits;i++) {
				segments.add(memoryManager.allocate(mockInvoke, MEMORY_SIZE/numSplits));
			}
		} catch (MemoryAllocationException e) {
			Assert.fail("Unable to allocate memory");
		}
		
		memoryManager.release(segments);
	}
	
	@Test
	public void allocateAllMulti() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		
		int numSplits = 6;
		if(MEMORY_SIZE%numSplits != 0) throw new Exception("Invalid number of splits");
		
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		
		try {
			for(int i=0;i<numSplits/2;i++) {
				segments.addAll(memoryManager.allocate(mockInvoke, 2, MEMORY_SIZE/numSplits));
			}
		} catch (MemoryAllocationException e) {
			Assert.fail("Unable to allocate memory");
		}
		
		memoryManager.release(segments);
	}
	
	@Test
	public void allocateAllMixed() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		
		memoryManager = new DefaultMemoryManager(MEMORY_SIZE);
		
		List<MemorySegment> segments = new ArrayList<MemorySegment>();
		
		try {
			segments.add(memoryManager.allocate(mockInvoke, 1024*1024*10));
			segments.add(memoryManager.allocate(mockInvoke, 1024*1024*10));
			segments.add(memoryManager.allocate(mockInvoke, 1024*1024*10));
			segments.addAll(memoryManager.allocate(mockInvoke, 2, 1024*1024*3));
			segments.add(memoryManager.allocate(mockInvoke, 1024*1024*10));
			segments.add(memoryManager.allocate(mockInvoke, 1024*1024*10));
			segments.add(memoryManager.allocate(mockInvoke, 1024*1024*10));
			segments.addAll(memoryManager.allocate(mockInvoke, 2, 1024*1024*3));
			
		} catch (MemoryAllocationException e) {
			Assert.fail("Unable to allocate memory");
		}
		
		memoryManager.release(segments);
	}
	
	@Test
	public void allocateRelease() throws Throwable {
		
		final AbstractInvokable memOwner = new DummyInvokable();
		MemorySegment segment = null;

		/*
		 * part 1: test allocation of more than the available memory
		 */
		try {
			segment = memoryManager.allocate(memOwner, CHUNK_SIZE + 1);
			memoryManager.release(segment);
			fail("MemoryManagementAllocation expected");
		} catch (MemoryAllocationException e) {
		}

		/*
		 * part 2: test allocation of the whole available memory
		 */
		try {
			segment = memoryManager.allocate(memOwner, CHUNK_SIZE);
			memoryManager.release(segment);
		} catch (MemoryAllocationException e) {
			fail("unexpected MemoryAllocationException");
		}

		/*
		 * part 3: test random allocation and release of different segment sizes
		 */
		for (int segmentSize : SEGMENT_SIZES) {
			ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>(MEMORY_SIZE / segmentSize);

			try {
				for (int i = 0; i < MEMORY_SIZE / segmentSize; i++) {
					segments.add(i, memoryManager.allocate(memOwner, segmentSize));
				}
			} catch (MemoryAllocationException e) {
				fail("unexpected MemoryAllocationException");
			} finally {
				Collections.shuffle(segments, random);
				for (MemorySegment s : segments) {
					memoryManager.release(s);
				}
			}
		}
	}

	@Test
	public void bulkAllocateRelease() {
		
		final AbstractInvokable memOwner = new DummyInvokable();
		List<MemorySegment> segments = null;

		/*
		 * part 1: test allocation of more than the available memory
		 */
		try {
			segments = memoryManager.allocate(memOwner, 9, MEMORY_SIZE / 8);
			memoryManager.release(segments);
			fail("MemoryManagementAllocation expected");
		} catch (MemoryAllocationException e) {
		}

		/*
		 * part 2: test random allocation and release of different segment sizes
		 */
		for (int segmentSize : SEGMENT_SIZES) {
			try {
				segments = memoryManager.allocate(memOwner, MEMORY_SIZE / segmentSize, segmentSize);
				memoryManager.release(segments);
			} catch (MemoryAllocationException e) {
				fail("unexpected MemoryManagementAllocation");
			}
		}
	}

	@Test
	public void testAutomaticReintegrationOfFreeSegments() throws Exception {
		
		final AbstractInvokable memOwner = new DummyInvokable();
		ArrayList<MemorySegment> segments = new ArrayList<MemorySegment>();

		try {
			// chunk 0
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 10)); // 00 (00-10)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 02)); // 01 (10-12)
			// chunk 1
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 04)); // 02 (12-16)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 04)); // 03 (16-20)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 04)); // 04 (20-24)
			// chunk 2
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 06)); // 05 (24-30)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 06)); // 06 (30-36)
			// chunk 3
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 04)); // 07 (36-40)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 04)); // 08 (40-44)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 04)); // 09 (44-48)
			// chunk 4
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 06)); // 10 (48-54)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 05)); // 11 (54-59)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 01)); // 12 (79-60)
			// chunk 5
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 12)); // 13 (60-72)

			memoryManager.release(segments.get(6)); // case 1 (memory manager empty)

			memoryManager.release(segments.get(3)); // case 2 (smaller than first, insert new)
			memoryManager.release(segments.get(2)); // case 2 (smaller than first, decrease start)

			memoryManager.release(segments.get(8)); // case 3 (lager than last, insert new segment)
			memoryManager.release(segments.get(9)); // case 3 (lager than last, increase upper border) 
			
			memoryManager.release(segments.get(0)); // case 2 (smaller than first, insert new free segment)
			memoryManager.release(segments.get(1)); // case 4 (extend upper border)
			memoryManager.release(segments.get(5)); // case 4 (extend lower border)
			
			memoryManager.release(segments.get(13)); // case 3 (larger than last, insert new free segment)
			
			memoryManager.release(segments.get(10)); // case 4 (insert new free segment)
			memoryManager.release(segments.get(12)); // case 4 (insert new free segment)
			memoryManager.release(segments.get(11)); // case 4 (merge left and right free segments)
			
			memoryManager.release(segments.get(4));  // case 4 (extend upper bound)
			memoryManager.release(segments.get(7));  // case 4 (extend lower bound)
			
			if (!memoryManager.verifyEmpty()) {
				fail("Memory manager is not complete empty and valid at the end of the test.");
			}

			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 12)); // 14 (00-12)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 12)); // 15 (12-24)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 12)); // 16 (24-36)
			segments.add(memoryManager.allocate(memOwner, 1024 * 1024 * 12)); // 17 (48-60)

			memoryManager.release(segments);
		} catch (MemoryAllocationException e) {
			fail("unexpected MemoryAllocationException");
		}
	}
	
	/**
	 * Utility class to serve as owner for the memory.
	 */
	public static final class DummyInvokable extends AbstractInvokable {
		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() throws Exception {}
	}
}
