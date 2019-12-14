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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BufferStorage}.
 */
public abstract class BufferStorageTestBase {

	protected static final int PAGE_SIZE = 4096;

	abstract BufferStorage createBufferStorage();

	@Test
	public void testRollOverEmptySequences() throws IOException {
		BufferStorage bufferStorage = createBufferStorage();
		bufferStorage.rollOver();
		assertFalse(bufferStorage.pollNext().isPresent());
		bufferStorage.rollOver();
		assertFalse(bufferStorage.pollNext().isPresent());
		bufferStorage.rollOver();
		assertFalse(bufferStorage.pollNext().isPresent());
	}

	@Test
	public void testSpillAndRollOverSimple() throws IOException {
		final Random rnd = new Random();
		final Random bufferRnd = new Random();

		final int maxNumEventsAndBuffers = 3000;
		final int maxNumChannels = 1656;

		BufferStorage bufferStorage = createBufferStorage();

		// do multiple spilling / rolling over rounds
		for (int round = 0; round < 5; round++) {

			final long bufferSeed = rnd.nextLong();
			bufferRnd.setSeed(bufferSeed);

			final int numEventsAndBuffers = rnd.nextInt(maxNumEventsAndBuffers) + 1;
			final int numberOfChannels = rnd.nextInt(maxNumChannels) + 1;

			final ArrayList<BufferOrEvent> events = new ArrayList<BufferOrEvent>(128);

			// generate sequence
			for (int i = 0; i < numEventsAndBuffers; i++) {
				boolean isEvent = rnd.nextDouble() < 0.05d;
				BufferOrEvent evt;
				if (isEvent) {
					evt = generateRandomEvent(rnd, numberOfChannels);
					events.add(evt);
				} else {
					evt = generateRandomBuffer(bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numberOfChannels));
				}
				bufferStorage.add(evt);
			}

			// reset and create reader
			bufferRnd.setSeed(bufferSeed);

			bufferStorage.rollOver();

			// read and validate the sequence

			int numEvent = 0;
			for (int i = 0; i < numEventsAndBuffers; i++) {
				assertFalse(bufferStorage.isEmpty());

				Optional<BufferOrEvent> next = bufferStorage.pollNext();
				assertTrue(next.isPresent());
				BufferOrEvent bufferOrEvent = next.get();

				if (bufferOrEvent.isEvent()) {
					BufferOrEvent expected = events.get(numEvent++);
					assertEquals(expected.getEvent(), bufferOrEvent.getEvent());
					assertEquals(expected.getChannelIndex(), bufferOrEvent.getChannelIndex());
				} else {
					validateBuffer(
						bufferOrEvent,
						bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numberOfChannels));
				}
			}

			// no further data
			assertFalse(bufferStorage.pollNext().isPresent());
			assertTrue(bufferStorage.isEmpty());

			// all events need to be consumed
			assertEquals(events.size(), numEvent);
		}
	}

	@Test
	public void testSpillWhileReading() throws IOException {
		final int sequences = 10;

		final Random rnd = new Random();

		final int maxNumEventsAndBuffers = 300;
		final int maxNumChannels = 1656;

		ArrayDeque<ArrayDeque<BufferOrEvent>> expectedRolledSequences = new ArrayDeque<>();
		ArrayDeque<BufferOrEvent> expectedPendingSequence = new ArrayDeque<>();

		BufferStorage bufferStorage = createBufferStorage();

		// do multiple spilling / rolling over rounds
		for (int round = 0; round < 2 * sequences; round++) {

			if (round % 2 == 1) {
				// make this an empty sequence
				bufferStorage.rollOver();
				expectedRolledSequences.addFirst(expectedPendingSequence);
				expectedPendingSequence = new ArrayDeque<>();
			} else {
				// proper spilled sequence
				final long bufferSeed = rnd.nextLong();
				final Random bufferRnd = new Random(bufferSeed);

				final int numEventsAndBuffers = rnd.nextInt(maxNumEventsAndBuffers) + 1;
				final int numberOfChannels = rnd.nextInt(maxNumChannels) + 1;

				final ArrayList<BufferOrEvent> events = new ArrayList<>(128);

				int generated = 0;
				while (generated < numEventsAndBuffers) {

					if (rnd.nextDouble() < 0.5) {
						// add a new record
						boolean isEvent = rnd.nextDouble() < 0.05;
						BufferOrEvent evt;
						if (isEvent) {
							evt = generateRandomEvent(rnd, numberOfChannels);
							events.add(evt);
						} else {
							evt = generateRandomBuffer(bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numberOfChannels));
						}
						bufferStorage.add(evt);

						expectedPendingSequence.addLast(evt);
						generated++;
					} else {
						// consume a record
						bufferStorage.rollOver();
						expectedRolledSequences.addFirst(expectedPendingSequence);
						expectedPendingSequence = new ArrayDeque<>();

						assertNextBufferOrEvent(expectedRolledSequences, bufferStorage);
					}
				}
				bufferStorage.rollOver();
				expectedRolledSequences.addFirst(expectedPendingSequence);
				expectedPendingSequence = new ArrayDeque<>();
			}
		}

		// consume all the remainder
		while (!expectedRolledSequences.isEmpty()) {
			assertNextBufferOrEvent(expectedRolledSequences, bufferStorage);
		}
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static void assertNextBufferOrEvent(
			ArrayDeque<ArrayDeque<BufferOrEvent>> expectedRolledSequence,
			BufferStorage bufferStorage) throws IOException {
		while (!expectedRolledSequence.isEmpty() && expectedRolledSequence.peekFirst().isEmpty()) {
			expectedRolledSequence.pollFirst();
		}

		Optional<BufferOrEvent> next = bufferStorage.pollNext();
		if (expectedRolledSequence.isEmpty()) {
			assertFalse(next.isPresent());
			return;
		}

		while (!next.isPresent() && !bufferStorage.isEmpty()) {
			next = bufferStorage.pollNext();
		}

		assertTrue(next.isPresent());
		BufferOrEvent actualBufferOrEvent = next.get();
		BufferOrEvent expectedBufferOrEvent = expectedRolledSequence.peekFirst().pollFirst();

		if (expectedBufferOrEvent.isEvent()) {
			assertEquals(expectedBufferOrEvent.getChannelIndex(), actualBufferOrEvent.getChannelIndex());
			assertEquals(expectedBufferOrEvent.getEvent(), actualBufferOrEvent.getEvent());
		} else {
			validateBuffer(
				actualBufferOrEvent,
				expectedBufferOrEvent.getSize(),
				expectedBufferOrEvent.getChannelIndex());
		}
	}

	private static BufferOrEvent generateRandomEvent(Random rnd, int numberOfChannels) {
		long magicNumber = rnd.nextLong();
		byte[] data = new byte[rnd.nextInt(1000)];
		rnd.nextBytes(data);
		TestEvent evt = new TestEvent(magicNumber, data);

		int channelIndex = rnd.nextInt(numberOfChannels);

		return new BufferOrEvent(evt, channelIndex);
	}

	public static BufferOrEvent generateRandomBuffer(int size) {
		return generateRandomBuffer(size, 0);
	}

	public static BufferOrEvent generateRandomBuffer(int size, int channelIndex) {
		MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(PAGE_SIZE);
		for (int i = 0; i < size; i++) {
			seg.put(i, (byte) i);
		}

		Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
		buf.setSize(size);
		return new BufferOrEvent(buf, channelIndex);
	}

	private static void validateBuffer(BufferOrEvent boe, int expectedSize, int expectedChannelIndex) {
		assertEquals("wrong channel index", expectedChannelIndex, boe.getChannelIndex());
		assertTrue("is not buffer", boe.isBuffer());

		Buffer buf = boe.getBuffer();
		assertEquals("wrong buffer size", expectedSize, buf.getSize());

		MemorySegment seg = buf.getMemorySegment();
		for (int i = 0; i < expectedSize; i++) {
			byte expected = (byte) i;
			if (expected != seg.get(i)) {
				fail(String.format(
					"wrong buffer contents at position %s : expected=%d , found=%d", i, expected, seg.get(i)));
			}
		}
	}
}
