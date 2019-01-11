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
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.streaming.runtime.io.BufferSpiller.SpilledBufferOrEventSequence;
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that validate the behavior of the {@link SpilledBufferOrEventSequence} in isolation,
 * with respect to detecting corrupt sequences, trailing data, and interleaved buffers and events.
 */
public class SpilledBufferOrEventSequenceTest {

	private final ByteBuffer buffer = ByteBuffer.allocateDirect(128 * 1024).order(ByteOrder.LITTLE_ENDIAN);
	private final int pageSize = 32 * 1024;

	private File tempFile;
	private FileChannel fileChannel;

	@Before
	public void initTempChannel() {
		try {
			tempFile = File.createTempFile("testdata", "tmp");
			fileChannel = new RandomAccessFile(tempFile, "rw").getChannel();
		}
		catch (Exception e) {
			cleanup();
		}
	}

	@After
	public void cleanup() {
		if (fileChannel != null) {
			try {
				fileChannel.close();
			}
			catch (IOException e) {
				// ignore
			}
		}
		if (tempFile != null) {
			//noinspection ResultOfMethodCallIgnored
			tempFile.delete();
		}
	}


	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testEmptyChannel() {
		try {
			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();

			assertNull(seq.getNext());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testIncompleteHeaderOnFirstElement() {
		try {
			ByteBuffer buf = ByteBuffer.allocate(7);
			buf.order(ByteOrder.LITTLE_ENDIAN);

			FileUtils.writeCompletely(fileChannel, buf);
			fileChannel.position(0);

			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();

			try {
				seq.getNext();
				fail("should fail with an exception");
			}
			catch (IOException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBufferSequence() {
		try {
			final Random rnd = new Random();
			final long seed = rnd.nextLong();

			final int numBuffers = 325;
			final int numChannels = 671;

			rnd.setSeed(seed);

			for (int i = 0; i < numBuffers; i++) {
				writeBuffer(fileChannel, rnd.nextInt(pageSize) + 1, rnd.nextInt(numChannels));
			}

			fileChannel.position(0L);
			rnd.setSeed(seed);

			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();

			for (int i = 0; i < numBuffers; i++) {
				validateBuffer(seq.getNext(), rnd.nextInt(pageSize) + 1, rnd.nextInt(numChannels));
			}

			// should have no more data
			assertNull(seq.getNext());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testBufferSequenceWithIncompleteBuffer() {
		try {
			writeBuffer(fileChannel, 1672, 7);

			// write an incomplete buffer
			ByteBuffer data = ByteBuffer.allocate(615);
			data.order(ByteOrder.LITTLE_ENDIAN);

			data.putInt(2);
			data.putInt(999);
			data.put((byte) 0);
			data.position(0);
			data.limit(312);
			FileUtils.writeCompletely(fileChannel, data);
			fileChannel.position(0L);

			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();

			// first one is valid
			validateBuffer(seq.getNext(), 1672, 7);

			// next one should fail
			try {
				seq.getNext();
				fail("should fail with an exception");
			}
			catch (IOException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testEventSequence() {
		try {
			final Random rnd = new Random();
			final int numEvents = 3000;
			final int numChannels = 1656;

			final ArrayList<BufferOrEvent> events = new ArrayList<BufferOrEvent>(numEvents);

			for (int i = 0; i < numEvents; i++) {
				events.add(generateAndWriteEvent(fileChannel, rnd, numChannels));
			}

			fileChannel.position(0L);
			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();

			int i = 0;
			BufferOrEvent boe;
			while ((boe = seq.getNext()) != null) {
				BufferOrEvent expected = events.get(i);
				assertTrue(boe.isEvent());
				assertEquals(expected.getEvent(), boe.getEvent());
				assertEquals(expected.getChannelIndex(), boe.getChannelIndex());
				i++;
			}

			assertEquals(numEvents, i);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMixedSequence() {
		try {
			final Random rnd = new Random();
			final Random bufferRnd = new Random();

			final long bufferSeed = rnd.nextLong();
			bufferRnd.setSeed(bufferSeed);

			final int numEventsAndBuffers = 3000;
			final int numChannels = 1656;

			final ArrayList<BufferOrEvent> events = new ArrayList<BufferOrEvent>(128);

			// generate sequence

			for (int i = 0; i < numEventsAndBuffers; i++) {
				boolean isEvent = rnd.nextDouble() < 0.05d;
				if (isEvent) {
					events.add(generateAndWriteEvent(fileChannel, rnd, numChannels));
				}
				else {
					writeBuffer(fileChannel, bufferRnd.nextInt(pageSize) + 1, bufferRnd.nextInt(numChannels));
				}
			}

			// reset and create reader

			fileChannel.position(0L);
			bufferRnd.setSeed(bufferSeed);
			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();

			// read and validate the sequence

			int numEvent = 0;
			for (int i = 0; i < numEventsAndBuffers; i++) {
				BufferOrEvent next = seq.getNext();
				if (next.isEvent()) {
					BufferOrEvent expected = events.get(numEvent++);
					assertEquals(expected.getEvent(), next.getEvent());
					assertEquals(expected.getChannelIndex(), next.getChannelIndex());
				}
				else {
					validateBuffer(next, bufferRnd.nextInt(pageSize) + 1, bufferRnd.nextInt(numChannels));
				}
			}

			// no further data
			assertNull(seq.getNext());

			// all events need to be consumed
			assertEquals(events.size(), numEvent);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultipleSequences() {
		File secondFile = null;
		FileChannel secondChannel = null;

		try {
			// create the second file channel
			secondFile = File.createTempFile("testdata", "tmp");
			secondChannel = new RandomAccessFile(secondFile, "rw").getChannel();

			final Random rnd = new Random();
			final Random bufferRnd = new Random();

			final long bufferSeed = rnd.nextLong();
			bufferRnd.setSeed(bufferSeed);

			final int numEventsAndBuffers1 = 272;
			final int numEventsAndBuffers2 = 151;

			final int numChannels = 1656;

			final ArrayList<BufferOrEvent> events1 = new ArrayList<BufferOrEvent>(128);
			final ArrayList<BufferOrEvent> events2 = new ArrayList<BufferOrEvent>(128);

			// generate sequence 1

			for (int i = 0; i < numEventsAndBuffers1; i++) {
				boolean isEvent = rnd.nextDouble() < 0.05d;
				if (isEvent) {
					events1.add(generateAndWriteEvent(fileChannel, rnd, numChannels));
				}
				else {
					writeBuffer(fileChannel, bufferRnd.nextInt(pageSize) + 1, bufferRnd.nextInt(numChannels));
				}
			}

			// generate sequence 2

			for (int i = 0; i < numEventsAndBuffers2; i++) {
				boolean isEvent = rnd.nextDouble() < 0.05d;
				if (isEvent) {
					events2.add(generateAndWriteEvent(secondChannel, rnd, numChannels));
				}
				else {
					writeBuffer(secondChannel, bufferRnd.nextInt(pageSize) + 1, bufferRnd.nextInt(numChannels));
				}
			}

			// reset and create reader

			fileChannel.position(0L);
			secondChannel.position(0L);

			bufferRnd.setSeed(bufferSeed);

			SpilledBufferOrEventSequence seq1 = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			SpilledBufferOrEventSequence seq2 = new SpilledBufferOrEventSequence(secondFile, secondChannel, buffer, pageSize);

			// read and validate the sequence 1
			seq1.open();

			int numEvent = 0;
			for (int i = 0; i < numEventsAndBuffers1; i++) {
				BufferOrEvent next = seq1.getNext();
				if (next.isEvent()) {
					BufferOrEvent expected = events1.get(numEvent++);
					assertEquals(expected.getEvent(), next.getEvent());
					assertEquals(expected.getChannelIndex(), next.getChannelIndex());
				}
				else {
					validateBuffer(next, bufferRnd.nextInt(pageSize) + 1, bufferRnd.nextInt(numChannels));
				}
			}
			assertNull(seq1.getNext());
			assertEquals(events1.size(), numEvent);

			// read and validate the sequence 2
			seq2.open();

			numEvent = 0;
			for (int i = 0; i < numEventsAndBuffers2; i++) {
				BufferOrEvent next = seq2.getNext();
				if (next.isEvent()) {
					BufferOrEvent expected = events2.get(numEvent++);
					assertEquals(expected.getEvent(), next.getEvent());
					assertEquals(expected.getChannelIndex(), next.getChannelIndex());
				}
				else {
					validateBuffer(next, bufferRnd.nextInt(pageSize) + 1, bufferRnd.nextInt(numChannels));
				}
			}
			assertNull(seq2.getNext());
			assertEquals(events2.size(), numEvent);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (secondChannel != null) {
				try {
					secondChannel.close();
				}
				catch (IOException e) {
					// ignore here
				}
			}
			if (secondFile != null) {
				//noinspection ResultOfMethodCallIgnored
				secondFile.delete();
			}
		}
	}

	@Test
	public void testCleanup() {
		try {
			ByteBuffer data = ByteBuffer.allocate(157);
			data.order(ByteOrder.LITTLE_ENDIAN);

			FileUtils.writeCompletely(fileChannel, data);
			fileChannel.position(54);

			SpilledBufferOrEventSequence seq = new SpilledBufferOrEventSequence(tempFile, fileChannel, buffer, pageSize);
			seq.open();
			seq.cleanup();

			assertFalse(fileChannel.isOpen());
			assertFalse(tempFile.exists());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static BufferOrEvent generateAndWriteEvent(FileChannel fileChannel, Random rnd, int numChannels) throws IOException {
		long magicNumber = rnd.nextLong();
		byte[] data = new byte[rnd.nextInt(1000)];
		rnd.nextBytes(data);
		TestEvent evt = new TestEvent(magicNumber, data);

		int channelIndex = rnd.nextInt(numChannels);

		ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(evt);
		ByteBuffer header = ByteBuffer.allocate(9);
		header.order(ByteOrder.LITTLE_ENDIAN);

		header.putInt(channelIndex);
		header.putInt(serializedEvent.remaining());
		header.put((byte) 1);
		header.flip();

		FileUtils.writeCompletely(fileChannel, header);
		FileUtils.writeCompletely(fileChannel, serializedEvent);
		return new BufferOrEvent(evt, channelIndex);
	}

	private static void writeBuffer(FileChannel fileChannel, int size, int channelIndex) throws IOException {
		ByteBuffer data = ByteBuffer.allocate(size + 9);
		data.order(ByteOrder.LITTLE_ENDIAN);

		data.putInt(channelIndex);
		data.putInt(size);
		data.put((byte) 0);
		for (int i = 0; i < size; i++) {
			data.put((byte) i);
		}
		data.flip();
		FileUtils.writeCompletely(fileChannel, data);
	}

	private static void validateBuffer(BufferOrEvent boe, int expectedSize, int expectedChannelIndex) {
		assertEquals("wrong channel index", expectedChannelIndex, boe.getChannelIndex());
		assertTrue("is not buffer", boe.isBuffer());

		Buffer buf = boe.getBuffer();
		assertEquals("wrong buffer size", expectedSize, buf.getSize());

		MemorySegment seg = buf.getMemorySegment();
		for (int i = 0; i < expectedSize; i++) {
			assertEquals("wrong buffer contents", (byte) i, seg.get(i));
		}
	}
}
