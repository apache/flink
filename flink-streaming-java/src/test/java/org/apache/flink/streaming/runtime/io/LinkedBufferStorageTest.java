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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

import static junit.framework.TestCase.assertFalse;
import static org.apache.flink.streaming.runtime.io.BufferStorageTestBase.generateRandomBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LinkedBufferStorage}.
 */
public class LinkedBufferStorageTest {
	private static final int PAGE_SIZE = 100;

	private CachedBufferStorage mainStorage;

	private CachedBufferStorage linkedStorage1;

	private CachedBufferStorage linkedStorage2;

	private LinkedBufferStorage bufferStorage;

	@Before
	public void setUp() {
		mainStorage = new CachedBufferStorage(PAGE_SIZE);
		linkedStorage1 = new CachedBufferStorage(PAGE_SIZE);
		linkedStorage2 = new CachedBufferStorage(PAGE_SIZE);
		bufferStorage = new LinkedBufferStorage(
			mainStorage,
			700,
			linkedStorage1,
			linkedStorage2);
	}

	@After
	public void tearDown() throws IOException {
		bufferStorage.close();
		mainStorage.close();
		linkedStorage1.close();
		linkedStorage2.close();
	}

	@Test
	public void testBasicUsage() throws IOException {
		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE + 0));
		assertEquals(PAGE_SIZE, bufferStorage.getPendingBytes());
		linkedStorage2.add(generateRandomBuffer(PAGE_SIZE + 1));
		assertEquals(PAGE_SIZE * 2, bufferStorage.getPendingBytes());

		assertTrue(bufferStorage.isEmpty());

		bufferStorage.add(generateRandomBuffer(PAGE_SIZE + 2));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE + 3));

		assertTrue(bufferStorage.isEmpty());
		assertPendingBytes();
		assertRolledBytes();

		assertTrue(bufferStorage.isEmpty());
		assertTrue(linkedStorage1.isEmpty());
		assertTrue(linkedStorage2.isEmpty());

		bufferStorage.rollOver();

		assertFalse(bufferStorage.isEmpty());
		assertFalse(linkedStorage1.isEmpty());
		assertFalse(linkedStorage2.isEmpty());

		assertPendingBytes();
		assertRolledBytes();

		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE + 4));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE + 5));

		assertPendingBytes();
		assertRolledBytes();

		bufferStorage.rollOver();

		assertPendingBytes();
		assertRolledBytes();

		ArrayList<Integer> bufferSizes = drain(bufferStorage);

		assertEquals(PAGE_SIZE + 5, (long) bufferSizes.get(0));
		assertEquals(PAGE_SIZE + 2, (long) bufferSizes.get(1));
		assertEquals(PAGE_SIZE + 3, (long) bufferSizes.get(2));

		bufferSizes = drain(linkedStorage1);

		assertEquals(PAGE_SIZE + 4, (long) bufferSizes.get(0));
		assertEquals(PAGE_SIZE + 0, (long) bufferSizes.get(1));

		bufferSizes = drain(linkedStorage2);

		assertEquals(PAGE_SIZE + 1, (long) bufferSizes.get(0));

		assertEquals(0, bufferStorage.getRolledBytes());
		assertEquals(0, bufferStorage.getPendingBytes());
	}

	@Test
	public void testPendingIsFull() throws IOException {
		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE));
		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));

		assertFalse(bufferStorage.isFull());

		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));

		assertTrue(bufferStorage.isFull());
	}

	/**
	 * This test is broken because of FLINK-12912.
	 * https://issues.apache.org/jira/browse/FLINK-12912
	 */
	//@Test
	public void testRolledIsFull() throws IOException {
		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE));
		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.rollOver();
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.rollOver();
		linkedStorage1.add(generateRandomBuffer(PAGE_SIZE));
		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));

		assertFalse(bufferStorage.isFull());

		bufferStorage.add(generateRandomBuffer(PAGE_SIZE));

		assertTrue(bufferStorage.isFull());
	}

	private void assertPendingBytes() {
		assertEquals(
			mainStorage.getPendingBytes() + linkedStorage1.getPendingBytes() + linkedStorage2.getPendingBytes(),
			bufferStorage.getPendingBytes());
	}

	private void assertRolledBytes() {
		assertEquals(
			mainStorage.getRolledBytes() + linkedStorage1.getRolledBytes() + linkedStorage2.getRolledBytes(),
			bufferStorage.getRolledBytes());
	}

	private ArrayList<Integer> drain(BufferStorage bufferStorage) throws IOException {
		ArrayList<Integer> result = new ArrayList<>();
		while (!bufferStorage.isEmpty()) {
			Optional<BufferOrEvent> bufferOrEvent = bufferStorage.pollNext();
			if (bufferOrEvent.isPresent()) {
				result.add(bufferOrEvent.get().getSize());
			}
		}
		return result;
	}
}
