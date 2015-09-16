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
package org.apache.flink.runtime.operators.sort;

import com.google.common.base.Strings;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.runtime.operators.testutils.types.IntPairComparator;
import org.apache.flink.runtime.operators.testutils.types.IntPairSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class QuickHeapPriorityQueueITCase {
	private static final int MEMORY_SIZE = 1024 * 32 * 9;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;

	private MemoryManager memoryManager;

	private TypeSerializer<IntPair> intPairSerializer;

	private TypeSerializer<String> stringSerializer;

	private TypeComparator<IntPair> intPairComparator;

	private TypeComparator<String> stringComparator;

	private IOManager ioManager;

	@Before
	public void init() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.intPairSerializer = new IntPairSerializer();
		this.intPairComparator = new IntPairComparator();
		this.stringSerializer = new StringSerializer();
		this.stringComparator = new StringComparator(true);
	}

	@After
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	// ------------------------------------------------------------------------------------------------
	// --------------------------------- Correctness verification ----------------------------------------
	// ------------------------------------------------------------------------------------------------

	@Test
	public void testPriorityQueueWithFixedLengthRecord1() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
			new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		try {
			heap.insert(new IntPair(1, 1));
			heap.insert(new IntPair(6, 6));
			heap.insert(new IntPair(2, 2));
			heap.insert(new IntPair(5, 5));
			heap.insert(new IntPair(0, 0));
			heap.insert(new IntPair(3, 3));
			heap.insert(new IntPair(4, 4));

			for (int i = 0; i < 7; i++) {
				IntPair next = heap.next();
				assertEquals(i, next.getKey());
			}
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueueWithFixedLengthRecord2() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
			new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		try {
			heap.insert(new IntPair(1, 1));
			heap.insert(new IntPair(6, 6));
			heap.insert(new IntPair(2, 2));
			heap.insert(new IntPair(5, 5));
			heap.insert(new IntPair(0, 0));
			heap.insert(new IntPair(3, 3));
			heap.insert(new IntPair(4, 4));
			heap.insert(new IntPair(2, 2));
			heap.insert(new IntPair(5, 5));

			IntPair next = heap.next();
			next = heap.next();
			next = heap.next();
			assertEquals(2, next.getKey());
			next = heap.next();
			assertEquals(2, next.getKey());
			next = heap.next();
			next = heap.next();
			next = heap.next();
			assertEquals(5, next.getKey());
			next = heap.next();
			assertEquals(5, next.getKey());
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueueWithFixedLength3() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
			new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		try {
			int recordNumber = (1024 * 32 * 4) / 8;

			for (int i = recordNumber - 1; i >= 0; i--) {
				heap.insert(new IntPair(i, i));
			}

			for (int i = 0; i < recordNumber; i++) {
				IntPair next = heap.next();
				assertEquals(i, next.getKey());
			}
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueueWithNormalizedKeyRecord1() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<String> heap =
			new QuickHeapPriorityQueue<>(memory, stringSerializer, stringComparator, memoryManager, ioManager);
		try {
			heap.insert("ee");
			heap.insert("bb");
			heap.insert("aa");
			heap.insert("ff");
			heap.insert("gg");
			heap.insert("cc");
			heap.insert("dd");

			String next = heap.next();
			assertEquals("aa", next);
			next = heap.next();
			assertEquals("bb", next);
			next = heap.next();
			assertEquals("cc", next);
			next = heap.next();
			assertEquals("dd", next);
			next = heap.next();
			assertEquals("ee", next);
			next = heap.next();
			assertEquals("ff", next);
			next = heap.next();
			assertEquals("gg", next);
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueueWithNormalizedKeyRecord2() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		StringComparator stringComparator2 = new StringComparator(false);
		QuickHeapPriorityQueue<String> heap =
			new QuickHeapPriorityQueue<>(memory, stringSerializer, stringComparator2, memoryManager, ioManager);
		try {
			heap.insert("ee");
			heap.insert("bb");
			heap.insert("aa");
			heap.insert("ff");
			heap.insert("gg");
			heap.insert("cc");
			heap.insert("dd");

			String next = heap.next();
			assertEquals("gg", next);
			next = heap.next();
			assertEquals("ff", next);
			next = heap.next();
			assertEquals("ee", next);
			next = heap.next();
			assertEquals("dd", next);
			next = heap.next();
			assertEquals("cc", next);
			next = heap.next();
			assertEquals("bb", next);
			next = heap.next();
			assertEquals("aa", next);
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueueWithNormalizedKeyRecord3() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<String> heap =
			new QuickHeapPriorityQueue<>(memory, stringSerializer, stringComparator, memoryManager, ioManager);
		try {
			heap.insert("ee");
			heap.insert("bb");
			heap.insert("aa");
			heap.insert("ff");
			heap.insert("gg");
			heap.insert("cc");
			heap.insert("dd");
			heap.insert("aa");
			heap.insert("ff");

			String next = heap.next();
			assertEquals("aa", next);
			next = heap.next();
			assertEquals("aa", next);
			next = heap.next();
			assertEquals("bb", next);
			next = heap.next();
			assertEquals("cc", next);
			next = heap.next();
			assertEquals("dd", next);
			next = heap.next();
			assertEquals("ee", next);
			next = heap.next();
			assertEquals("ff", next);
			next = heap.next();
			assertEquals("ff", next);
			next = heap.next();
			assertEquals("gg", next);
		} finally {
			heap.close();
		}
	}

	// ------------------------------------------------------------------------------------------------
	// --------------------------------- Boundary verification ----------------------------------------
	// ------------------------------------------------------------------------------------------------

	@Test
	public void testEmptyPriorityQueue() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
			new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		try {
			IntPair next = heap.next();
			assertNull(next);
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueue1() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
			new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		try {
			heap.insert(new IntPair(1, 1));
			IntPair next = heap.next();
			assertEquals(1, next.getKey());
			next = heap.next();
			assertNull(next);
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueue2() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
			new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		try {
			int recordNumber = (1024 * 32 * 3) / 8;

			for (int i = recordNumber - 1; i >= 0; i--) {
				heap.insert(new IntPair(i, i));
			}

			// IntPair has 8 bytes, so all the memory has been used, we could not insert any more records.
			heap.insert(new IntPair(recordNumber, recordNumber));

			for (int i = 0; i < recordNumber; i++) {
				IntPair next = heap.next();
				assertEquals(i, next.getKey());
			}
		} finally {
			heap.close();
		}
	}

	@Test
	public void testPriorityQueueWithNormalizedKeyRecord4() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<String> heap =
			new QuickHeapPriorityQueue<>(memory, stringSerializer, stringComparator, memoryManager, ioManager);
		try {
			heap.insert(Strings.repeat("e", 15 * 1024));
			heap.insert(Strings.repeat("b", 15 * 1024));
			heap.insert(Strings.repeat("a", 15 * 1024));
			heap.insert(Strings.repeat("f", 15 * 1024));
			// heap already full, neel to flush while insert more.
			heap.insert(Strings.repeat("c", 15 * 1024));
			heap.insert(Strings.repeat("d", 15 * 1024));

			String next = heap.next();
			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("b", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("c", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("d", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("e", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("f", 15 * 1024), next);
		} finally {
			heap.close();
		}
	}


	@Test
	public void testPriorityQueueWithNormalizedKeyRecord5() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<String> heap =
			new QuickHeapPriorityQueue<>(memory, stringSerializer, stringComparator, memoryManager, ioManager);
		try {
			heap.insert(Strings.repeat("a", 15 * 1024));
			heap.insert(Strings.repeat("a", 15 * 1024));
			heap.insert(Strings.repeat("a", 15 * 1024));
			heap.insert(Strings.repeat("a", 15 * 1024));

			heap.insert(Strings.repeat("b", 15 * 1024));
			heap.insert(Strings.repeat("a", 15 * 1024));
			heap.insert(Strings.repeat("a", 15 * 1024));
			// heap already full, neel to flush while insert more.
			String next = heap.next();

			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("a", 15 * 1024), next);
			next = heap.next();
			assertEquals(Strings.repeat("b", 15 * 1024), next);
		} finally {
			heap.close();
		}
	}
}

