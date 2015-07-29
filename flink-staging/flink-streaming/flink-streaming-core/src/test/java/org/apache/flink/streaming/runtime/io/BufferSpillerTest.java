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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class BufferSpillerTest {

	private static final int PAGE_SIZE = 4096;

	private static IOManager IO_MANAGER;

	private BufferSpiller spiller;


	// ------------------------------------------------------------------------
	//  Setup / Cleanup
	// ------------------------------------------------------------------------
	
	@BeforeClass
	public static void setupIOManager() {
		IO_MANAGER = new IOManagerAsync();
	}

	@AfterClass
	public static void shutdownIOManager() {
		IO_MANAGER.shutdown();
	}
	
	@Before
	public void createSpiller() {
		try {
			spiller = new BufferSpiller(IO_MANAGER, PAGE_SIZE);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Cannot create BufferSpiller: " + e.getMessage());
		}
	}
	
	@After
	public void cleanupSpiller() {
		if (spiller != null) {
			try {
				spiller.close();
			}
			catch (Exception e) {
				e.printStackTrace();
				fail("Cannot properly close the BufferSpiller: " + e.getMessage());
			}
			
			assertFalse(spiller.getCurrentChannel().isOpen());
			assertFalse(spiller.getCurrentSpillFile().exists());
		}
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------
	
	@Test
	public void testRollOverEmptySequences() {
		try {
			assertNull(spiller.rollOver());
			assertNull(spiller.rollOver());
			assertNull(spiller.rollOver());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSpillAndRollOverSimple() {
		try {
			final Random rnd = new Random();
			final Random bufferRnd = new Random();

			final int maxNumEventsAndBuffers = 3000;
			final int maxNumChannels = 1656;

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
					if (isEvent) {
						BufferOrEvent evt = generateRandomEvent(rnd, numChannels);
						events.add(evt);
						spiller.add(evt);
					}
					else {
						BufferOrEvent evt = generateRandomBuffer(bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numChannels));
						spiller.add(evt);
					}
				}

				// reset and create reader
				bufferRnd.setSeed(bufferSeed);
			
				BufferSpiller.SpilledBufferOrEventSequence seq = spiller.rollOver();
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
					}
					else {
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
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSpillWhileReading() {
		try {
			final int sequences = 10;
			
			final Random rnd = new Random();
			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
			
			final SequenceConsumer consumer = new SequenceConsumer(error, sequences);
			consumer.start();
			
			final int maxNumEventsAndBuffers = 30000;
			final int maxNumChannels = 1656;
			
			// do multiple spilling / rolling over rounds
			for (int round = 0; round < 2*sequences; round++) {

				if (round % 2 == 1) {
					// make this an empty sequence
					assertNull(spiller.rollOver());
				}
				else {
					// proper spilled sequence
					final long bufferSeed = rnd.nextLong();
					final Random bufferRnd = new Random(bufferSeed);
					
					final int numEventsAndBuffers = rnd.nextInt(maxNumEventsAndBuffers) + 1;
					final int numChannels = rnd.nextInt(maxNumChannels) + 1;
	
					final ArrayList<BufferOrEvent> events = new ArrayList<BufferOrEvent>(128);
	
					// generate sequence
					for (int i = 0; i < numEventsAndBuffers; i++) {
						boolean isEvent = rnd.nextDouble() < 0.05d;
						if (isEvent) {
							BufferOrEvent evt = generateRandomEvent(rnd, numChannels);
							events.add(evt);
							spiller.add(evt);
						}
						else {
							BufferOrEvent evt = generateRandomBuffer(bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numChannels));
							spiller.add(evt);
						}
					}
	
					// reset and create reader
					bufferRnd.setSeed(bufferSeed);
					BufferSpiller.SpilledBufferOrEventSequence seq = spiller.rollOver();
					
					SequenceToConsume stc = new SequenceToConsume(bufferRnd, events, seq, numEventsAndBuffers, numChannels);
					consumer.queue(stc);
				}
			}
			
			// wait for the consumer
			consumer.join(180000);
			assertFalse("sequence consumer did not finish its work in time", consumer.isAlive());
			
			// validate there was no error in the consumer
			if (error.get() != null) {
				Throwable t = error.get();
				if (t instanceof Error) {
					throw (Error) t;
				}
				else {
					throw new Exception("Error while consuming the spilled records", t);
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
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

	private static BufferOrEvent generateRandomBuffer(int size, int channelIndex) {
		MemorySegment seg = new MemorySegment(new byte[PAGE_SIZE]);
		for (int i = 0; i < size; i++) {
			seg.put(i, (byte) i);
		}
		
		Buffer buf = new Buffer(seg, FreeingBufferRecycler.INSTANCE);
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
			assertEquals("wrong buffer contents", (byte) i, seg.get(i));
		}
	}
	
	// ------------------------------------------------------------------------
	//  Async Consumer
	// ------------------------------------------------------------------------
	
	private static class SequenceToConsume {

		final BufferSpiller.SpilledBufferOrEventSequence sequence;
		final ArrayList<BufferOrEvent> events;
		final Random bufferRnd;
		final int numBuffersAndEvents;
		final int numChannels;

		private SequenceToConsume(Random bufferRnd, ArrayList<BufferOrEvent> events,
									BufferSpiller.SpilledBufferOrEventSequence sequence,
									int numBuffersAndEvents, int numChannels) {
			this.bufferRnd = bufferRnd;
			this.events = events;
			this.sequence = sequence;
			this.numBuffersAndEvents = numBuffersAndEvents;
			this.numChannels = numChannels;
		}
	}
	
	private static class SequenceConsumer extends Thread {
		
		private final AtomicReference<Throwable> error;
		private final BlockingQueue<SequenceToConsume> sequences;
		
		private final int numSequencesToConsume;
		
		private int consumedSequences;

		private SequenceConsumer(AtomicReference<Throwable> error, int numSequencesToConsume) {
			super("Sequence Consumer");
			setDaemon(true);
			
			this.error = error;
			this.numSequencesToConsume = numSequencesToConsume;
			this.sequences = new LinkedBlockingQueue<SequenceToConsume>();
		}


		@Override
		public void run() {
			try {
				while (consumedSequences < numSequencesToConsume) {
					// get next sequence
					SequenceToConsume nextSequence = sequences.take();
				
					// wait a bit, allow some stuff to queue up
					Thread.sleep(50);

					BufferSpiller.SpilledBufferOrEventSequence seq = nextSequence.sequence;
					ArrayList<BufferOrEvent> events = nextSequence.events;
					Random bufferRnd = nextSequence.bufferRnd;
					int numBuffersAndEvents = nextSequence.numBuffersAndEvents;
					int numChannels = nextSequence.numChannels;

					// consume sequence
					seq.open();
					
					int numEvent = 0;
					for (int i = 0; i < numBuffersAndEvents; i++) {
						BufferOrEvent next = seq.getNext();
						assertNotNull(next);
						if (next.isEvent()) {
							BufferOrEvent expected = events.get(numEvent++);
							assertEquals(expected.getEvent(), next.getEvent());
							assertEquals(expected.getChannelIndex(), next.getChannelIndex());
						}
						else {
							validateBuffer(next, bufferRnd.nextInt(PAGE_SIZE) + 1, bufferRnd.nextInt(numChannels));
						}
					}
	
					// no further data
					assertNull(seq.getNext());
	
					// all events need to be consumed
					assertEquals(events.size(), numEvent);
	
					// remove all temp files
					seq.cleanup();
					
					consumedSequences++;
				}
				
			}
			catch (Throwable t) {
				error.set(t);
			}
		}
		
		public void queue(SequenceToConsume next) {
			sequences.add(next);
		}
	}
}
