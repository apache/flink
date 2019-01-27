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
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests the memory manager for allocating both core and floating segments in
 * two different modes separately.
 */
@RunWith(Parameterized.class)
public class MemoryManagerTest {

	private static final long RANDOM_SEED = 643196033469871L;

	private static final int CORE_MEMORY_SIZE = 1024 * 1024 * 72; // 72 MiBytes

	private static final int PAGE_SIZE = 1024 * 32; // 32 KiBytes

	private static final int NUM_CORE_PAGES = CORE_MEMORY_SIZE / PAGE_SIZE;

	private final int floatingMemorySize;

	private final int numFloatingPages;

	private final boolean preAllocateMemory;

	private MemoryManager memoryManager;

	private Random random;

	@Parameterized.Parameters
	public static Collection<Object[]> getParams() {
		return Arrays.asList(new Object[][] {
			{1024 * 1024 * 16, true},
			{1024 * 1024 * 16, false},
			{0, true},
			{0, false},
		});
	}

	public MemoryManagerTest(int floatingMemorySize, boolean preAllocateMemory) {
		this.floatingMemorySize = floatingMemorySize;
		this.numFloatingPages = floatingMemorySize / PAGE_SIZE;
		this.preAllocateMemory = preAllocateMemory;
	}

	@Before
	public void setUp() {
		this.memoryManager = new MemoryManager(
			CORE_MEMORY_SIZE, floatingMemorySize, 1, PAGE_SIZE, MemoryType.HEAP, preAllocateMemory);
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
	public void allocateAllCorePagesBySingle() {
		allocateAllSingle(true);
	}

	@Test
	public void allocateAllFloatingPagesBySingle() {
		allocateAllSingle(false);
	}

	@Test
	public void allocateAllCorePagesByMulti() {
		allocateAllMulti(true);
	}

	@Test
	public void allocateAllFloatingPagesByMulti() {
		allocateAllMulti(false);
	}

	@Test
	public void allocateCorePagesForMultipleOwners() throws Exception {
		allocateMultipleOwners(true);
	}

	@Test
	public void allocateFloatingPagesForMultipleOwners() throws Exception {
		allocateMultipleOwners(false);
	}

	@Test
	public void allocateCorePagesTooMuch() throws Exception {
		allocateTooMuch(true);
	}

	@Test
	public void allocateFloatingPagesTooMuch() throws Exception {
		allocateTooMuch(false);
	}

	@Test
	public void releaseSingleOnlyIfContains() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();

		boolean[][] applyCoreAndReleaseCoreTypes = new boolean[][] {
			{true, true},
			{true, false},
			{false, true},
			{false, false}
		};

		for (boolean[] applyCoreAndReleaseCore : applyCoreAndReleaseCoreTypes) {
			int numPages = applyCoreAndReleaseCore[0] ? NUM_CORE_PAGES : floatingMemorySize;
			if (numPages > 0) {
				MemorySegment memorySegment = memoryManager.allocatePages(mockInvoke, 1, applyCoreAndReleaseCore[0]).get(0);
				boolean hasReleased = memoryManager.release(memorySegment, applyCoreAndReleaseCore[1]);
				assertEquals(applyCoreAndReleaseCore[0] == applyCoreAndReleaseCore[1], hasReleased);

				// Ensure released for passing the memory manager valid check.
				if (!hasReleased) {
					memoryManager.release(memorySegment, applyCoreAndReleaseCore[0]);
				}
			}
		}
	}

	@Test
	public void releaseMultipleCoreOnlyIfContains() throws Exception {
		releaseMultipleOnlyIfContains(true);
	}

	@Test
	public void releaseMultipleFloatingOnlyIfContains() throws Exception {
		releaseMultipleOnlyIfContains(false);
	}

	private void allocateAllSingle(boolean isCore) {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		final List<MemorySegment> segments = new ArrayList<>();
		final int numPages = isCore ? NUM_CORE_PAGES : numFloatingPages;

		try {
			for (int i = 0; i < numPages; i++) {
				segments.add(this.memoryManager.allocatePages(mockInvoke, 1, isCore).get(0));
			}
		} catch (MemoryAllocationException e) {
			fail("Unable to allocate memory");
		}

		this.memoryManager.release(segments, isCore);
	}

	private void allocateAllMulti(boolean isCore) {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		final List<MemorySegment> segments = new ArrayList<>();
		final int numPages = isCore ? NUM_CORE_PAGES : numFloatingPages;

		try {
			for (int i = 0; i < numPages / 2; i++) {
				segments.addAll(this.memoryManager.allocatePages(mockInvoke, 2, isCore));
			}
		} catch (MemoryAllocationException e) {
			Assert.fail("Unable to allocate memory");
		}

		this.memoryManager.release(segments, isCore);
	}

	private void allocateMultipleOwners(boolean isCore) throws Exception {
		final int numOwners = 17;
		final AbstractInvokable[] owners = new AbstractInvokable[numOwners];
		@SuppressWarnings("unchecked")
		final List<MemorySegment>[] segments = (List<MemorySegment>[]) new List<?>[numOwners];

		for (int i = 0; i < numOwners; i++) {
			owners[i] = new DummyInvokable();
			segments[i] = new ArrayList<>(64);
		}

		// allocate all memory to the different owners
		int numPages = isCore ? NUM_CORE_PAGES : numFloatingPages;
		for (int i = 0; i < numPages; i++) {
			final int owner = this.random.nextInt(numOwners);
			segments[owner].addAll(this.memoryManager.allocatePages(owners[owner], 1, isCore));
		}

		// free one owner at a time
		for (int i = 0; i < numOwners; i++) {
			this.memoryManager.releaseAll(owners[i]);
			owners[i] = null;
			Assert.assertTrue("Released memory segments have not been destroyed.", allMemorySegmentsFreed(segments[i]));
			segments[i] = null;

			// check that the other owners were not affected
			for (int k = i + 1; k < numOwners; k++) {
				Assert.assertTrue("Non-released memory segments are accidentaly destroyed.", allMemorySegmentsValid(segments[k]));
			}
		}
	}

	private void allocateTooMuch(boolean isCore) throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		final int numPages = isCore ? NUM_CORE_PAGES : numFloatingPages;
		final List<MemorySegment> segments = this.memoryManager.allocatePages(mockInvoke, numPages, isCore);

		try {
			this.memoryManager.allocatePages(mockInvoke, 1, isCore);
			Assert.fail("Expected MemoryAllocationException.");
		} catch (MemoryAllocationException maex) {
			// expected
		}

		Assert.assertTrue("The previously allocated segments were not valid any more.",
			allMemorySegmentsValid(segments));

		this.memoryManager.releaseAll(mockInvoke);
	}

	private void releaseMultipleOnlyIfContains(boolean releaseCore) throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();

		List<MemorySegment> coreSegments = memoryManager.allocatePages(mockInvoke, NUM_CORE_PAGES, true);
		List<MemorySegment> floatingSegments = memoryManager.allocatePages(mockInvoke, numFloatingPages, false);

		List<MemorySegment> allMemorySegments = new ArrayList<>();
		allMemorySegments.addAll(coreSegments);
		allMemorySegments.addAll(floatingSegments);
		Collections.shuffle(allMemorySegments);

		memoryManager.release(allMemorySegments, releaseCore);

		assertEquals(releaseCore ? numFloatingPages : NUM_CORE_PAGES, allMemorySegments.size());
		assertEquals(releaseCore ? new HashSet<>(floatingSegments) : new HashSet<>(coreSegments),
			new HashSet<>(allMemorySegments));

		memoryManager.release(allMemorySegments, !releaseCore);
		assertEquals(0, allMemorySegments.size());
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
}
