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
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BufferBlocker}.
 */
public abstract class BufferBlockerTestBase {

	protected static final int PAGE_SIZE = 4096;

	abstract BufferBlocker createBufferBlocker();

	@Test
	public void testRollOverEmptySequences() throws IOException {
		BufferBlocker bufferBlocker = createBufferBlocker();
		assertNull(bufferBlocker.rollOverReusingResources());
		assertNull(bufferBlocker.rollOverReusingResources());
		assertNull(bufferBlocker.rollOverReusingResources());
	}

	@Test
	public void testSpillAndRollOverSimple() throws IOException {
		final Random rnd = new Random();
		final Random bufferRnd = new Random();

		final int maxNumEventsAndBuffers = 3000;
		final int maxNumChannels = 1656;

		BufferBlocker bufferBlocker = createBufferBlocker();

		// do multiple spilling / rolling over rounds
		for (int round = 0; round < 5; round++) {

			final long bufferSeed = rnd.nextLong();
			bufferRnd.setSeed(bufferSeed);

			final int numEventsAndBuffers = rnd.nextInt(maxNumEventsAndBuffers) + 1;
			final int numChannels = rnd.nextInt(maxNumChannels) + 1;

			final ArrayList<BufferOrEvent> events = new ArrayList<BufferOrEvent>(128);

			// generate sequence
			for (int i = 0; i < numEventsAndBuffers; i++) {
				boolean isEvent = rnd.nextDouble() < 0.05d;
				BufferOrEvent evt;
				if (isEvent) {
					evt = generateRandomEvent(rnd, numChannels);
					events.add(evt);
				} else {
					evt = generateRandomBuffer(bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numChannels));
				}
				bufferBlocker.add(evt);
			}

			// reset and create reader
			bufferRnd.setSeed(bufferSeed);

			BufferOrEventSequence seq = bufferBlocker.rollOverReusingResources();
			seq.open();

			// read and validate the sequence

			int numEvent = 0;
			for (int i = 0; i < numEventsAndBuffers; i++) {
				BufferOrEvent next = seq.getNext();
				assertNotNull(next);
				if (next.isEvent()) {
					BufferOrEvent expected = events.get(numEvent++);
					assertEquals(expected.getEvent(), next.getEvent());
					assertEquals(expected.getChannelIndex(), next.getChannelIndex());
				} else {
					validateBuffer(next, bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numChannels));
				}
			}

			// no further data
			assertNull(seq.getNext());

			// all events need to be consumed
			assertEquals(events.size(), numEvent);

			seq.cleanup();
		}
	}

	@Test
	public void testSpillWhileReading() throws IOException {
		final int sequences = 10;

		final Random rnd = new Random();

		final int maxNumEventsAndBuffers = 30000;
		final int maxNumChannels = 1656;

		int sequencesConsumed = 0;

		ArrayDeque<SequenceToConsume> pendingSequences = new ArrayDeque<SequenceToConsume>();
		SequenceToConsume currentSequence = null;
		int currentNumEvents = 0;
		int currentNumRecordAndEvents = 0;

		BufferBlocker bufferBlocker = createBufferBlocker();

		// do multiple spilling / rolling over rounds
		for (int round = 0; round < 2 * sequences; round++) {

			if (round % 2 == 1) {
				// make this an empty sequence
				assertNull(bufferBlocker.rollOverReusingResources());
			} else {
				// proper spilled sequence
				final long bufferSeed = rnd.nextLong();
				final Random bufferRnd = new Random(bufferSeed);

				final int numEventsAndBuffers = rnd.nextInt(maxNumEventsAndBuffers) + 1;
				final int numChannels = rnd.nextInt(maxNumChannels) + 1;

				final ArrayList<BufferOrEvent> events = new ArrayList<BufferOrEvent>(128);

				int generated = 0;
				while (generated < numEventsAndBuffers) {

					if (currentSequence == null || rnd.nextDouble() < 0.5) {
						// add a new record
						boolean isEvent = rnd.nextDouble() < 0.05;
						BufferOrEvent evt;
						if (isEvent) {
							evt = generateRandomEvent(rnd, numChannels);
							events.add(evt);
						} else {
							evt = generateRandomBuffer(bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numChannels));
						}
						bufferBlocker.add(evt);
						generated++;
					} else {
						// consume a record
						BufferOrEvent next = currentSequence.sequence.getNext();
						assertNotNull(next);
						if (next.isEvent()) {
							BufferOrEvent expected = currentSequence.events.get(currentNumEvents++);
							assertEquals(expected.getEvent(), next.getEvent());
							assertEquals(expected.getChannelIndex(), next.getChannelIndex());
						} else {
							Random validationRnd = currentSequence.bufferRnd;
							validateBuffer(next, validationRnd.nextInt(PAGE_SIZE) + 1, validationRnd.nextInt(currentSequence.numChannels));
						}

						currentNumRecordAndEvents++;
						if (currentNumRecordAndEvents == currentSequence.numBuffersAndEvents) {
							// done with the sequence
							currentSequence.sequence.cleanup();
							sequencesConsumed++;

							// validate we had all events
							assertEquals(currentSequence.events.size(), currentNumEvents);

							// reset
							currentSequence = pendingSequences.pollFirst();
							if (currentSequence != null) {
								currentSequence.sequence.open();
							}

							currentNumRecordAndEvents = 0;
							currentNumEvents = 0;
						}
					}
				}

				// done generating a sequence. queue it for consumption
				bufferRnd.setSeed(bufferSeed);
				BufferOrEventSequence seq = bufferBlocker.rollOverReusingResources();

				SequenceToConsume stc = new SequenceToConsume(bufferRnd, events, seq, numEventsAndBuffers, numChannels);

				if (currentSequence == null) {
					currentSequence = stc;
					stc.sequence.open();
				} else {
					pendingSequences.addLast(stc);
				}
			}
		}

		// consume all the remainder
		while (currentSequence != null) {
			// consume a record
			BufferOrEvent next = currentSequence.sequence.getNext();
			assertNotNull(next);
			if (next.isEvent()) {
				BufferOrEvent expected = currentSequence.events.get(currentNumEvents++);
				assertEquals(expected.getEvent(), next.getEvent());
				assertEquals(expected.getChannelIndex(), next.getChannelIndex());
			} else {
				Random validationRnd = currentSequence.bufferRnd;
				validateBuffer(next, validationRnd.nextInt(PAGE_SIZE) + 1, validationRnd.nextInt(currentSequence.numChannels));
			}

			currentNumRecordAndEvents++;
			if (currentNumRecordAndEvents == currentSequence.numBuffersAndEvents) {
				// done with the sequence
				currentSequence.sequence.cleanup();
				sequencesConsumed++;

				// validate we had all events
				assertEquals(currentSequence.events.size(), currentNumEvents);

				// reset
				currentSequence = pendingSequences.pollFirst();
				if (currentSequence != null) {
					currentSequence.sequence.open();
				}

				currentNumRecordAndEvents = 0;
				currentNumEvents = 0;
			}
		}

		assertEquals(sequences, sequencesConsumed);
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static BufferOrEvent generateRandomEvent(Random rnd, int numChannels) {
		long magicNumber = rnd.nextLong();
		byte[] data = new byte[rnd.nextInt(1000)];
		rnd.nextBytes(data);
		TestEvent evt = new TestEvent(magicNumber, data);

		int channelIndex = rnd.nextInt(numChannels);

		return new BufferOrEvent(evt, channelIndex);
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

	/**
	 * Wrappers the buffered sequence and related elements for consuming and validation.
	 */
	private static class SequenceToConsume {

		final BufferOrEventSequence sequence;
		final ArrayList<BufferOrEvent> events;
		final Random bufferRnd;
		final int numBuffersAndEvents;
		final int numChannels;

		private SequenceToConsume(
				Random bufferRnd,
				ArrayList<BufferOrEvent> events,
				BufferOrEventSequence sequence,
				int numBuffersAndEvents,
				int numChannels) {
			this.bufferRnd = bufferRnd;
			this.events = events;
			this.sequence = sequence;
			this.numBuffersAndEvents = numBuffersAndEvents;
			this.numChannels = numChannels;
		}
	}
}
