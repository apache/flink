/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.network.envelope;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.DiscardingRecycler;
import eu.stratosphere.nephele.util.TestBufferProvider;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProvider;
import eu.stratosphere.runtime.io.network.bufferprovider.BufferProviderBroker;
import eu.stratosphere.runtime.io.BufferRecycler;
import eu.stratosphere.runtime.io.channels.ChannelID;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class EnvelopeReaderWriterTest {
	
	private final long RANDOM_SEED = 520346508276087l;

	private static final int BUFFER_SIZE = 32768;
	
	private static final byte BUFFER_CONTENT = 13;
	
	private final int[] BUFFER_SIZES = { 0, 2, BUFFER_SIZE, 3782, 88, 0, 23};
	
	private final AbstractEvent[][] EVENT_LISTS = {
		{},
		{},
		{},
		{ new TestEvent1(34872527) },
		{ new TestEvent1(8749653), new TestEvent1(365345) },
		{ new TestEvent2(34563456), new TestEvent1(598432), new TestEvent2(976293845) },
		{}
	};

	@Test
	public void testWriteAndRead() {
		
		Assert.assertTrue("Test broken.", BUFFER_SIZES.length == EVENT_LISTS.length);

		File testFile = null;
		RandomAccessFile raf = null;
		try {
			testFile = File.createTempFile("envelopes", ".tmp");
			raf = new RandomAccessFile(testFile, "rw");
			
			// write
			FileChannel c = raf.getChannel();
			writeEnvelopes(c);
			
			// read
			c.position(0);
			readEnvelopes(c, -1.0f);
			c.close();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		finally {
			if (raf != null)
				try { raf.close(); } catch (Throwable t) {}
			
			if (testFile != null)
				testFile.delete();
		}
	}
	
	@Test
	public void testWriteAndReadChunked() {
		
		Assert.assertTrue("Test broken.", BUFFER_SIZES.length == EVENT_LISTS.length);

		File testFile = null;
		RandomAccessFile raf = null;
		try {
			testFile = File.createTempFile("envelopes", ".tmp");
			raf = new RandomAccessFile(testFile, "rw");
			
			// write
			FileChannel c = raf.getChannel();
			writeEnvelopes(new ChunkedWriteableChannel(c));
			
			// read
			c.position(0);
			readEnvelopes(new ChunkedReadableChannel(c), 0.75f);
			c.close();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		finally {
			if (raf != null)
				try { raf.close(); } catch (Throwable t) {}
			
			if (testFile != null)
				testFile.delete();
		}
	}

	private void writeEnvelopes(WritableByteChannel channel) throws IOException {

		final BufferRecycler recycler = new DiscardingRecycler();
		final Random rand = new Random(RANDOM_SEED);
		
		final EnvelopeWriter serializer = new EnvelopeWriter();
		
		final int NUM_ENVS = BUFFER_SIZES.length;
		
		for (int i = 0; i < NUM_ENVS; i++) {
			int seqNum = Math.abs(rand.nextInt());
			JobID jid = new JobID(rand.nextLong(), rand.nextLong());
			ChannelID sid = new ChannelID(rand.nextLong(), rand.nextLong());
			
			Envelope env = new Envelope(seqNum, jid, sid);
			if (EVENT_LISTS[i].length > 0) {
				env.serializeEventList(Arrays.asList(EVENT_LISTS[i]));
			}
			
			int bufferSize = BUFFER_SIZES[i];
			if (bufferSize > 0) {
				MemorySegment ms = new MemorySegment(new byte[BUFFER_SIZE]);
				for (int x = 0; x < bufferSize; x++) {
					ms.put(x, BUFFER_CONTENT);
				}
				
				Buffer mb = new Buffer(ms, bufferSize, recycler);
				env.setBuffer(mb);
			}
			
			serializer.setEnvelopeForWriting(env);
			
			while (serializer.writeNextChunk(channel));
		}
	}
	
	private void readEnvelopes(ReadableByteChannel channel, float probabilityForNoBufferCurrently) throws IOException {
		
		final Random rand = new Random(RANDOM_SEED);
		
		final EnvelopeReader reader = new EnvelopeReader(new OneForAllBroker(BUFFER_SIZE, probabilityForNoBufferCurrently));
		
		final int NUM_ENVS = BUFFER_SIZES.length;
		
		for (int i = 0; i < NUM_ENVS; i++) {
			int expectedSeqNum = Math.abs(rand.nextInt());
			JobID expectedJid = new JobID(rand.nextLong(), rand.nextLong());
			ChannelID expectedSid = new ChannelID(rand.nextLong(), rand.nextLong());
			
			// read the next envelope
			while (reader.readNextChunk(channel) != EnvelopeReader.DeserializationState.COMPLETE);
			Envelope env = reader.getFullyDeserializedTransferEnvelope();
			
			// check the basic fields from the header
			Assert.assertEquals(expectedSeqNum, env.getSequenceNumber());
			Assert.assertEquals(expectedJid, env.getJobID());
			Assert.assertEquals(expectedSid, env.getSource());
			
			// check the events
			List<? extends AbstractEvent> events = env.deserializeEvents();
			Assert.assertEquals(EVENT_LISTS[i].length, events.size());
			
			for (int n = 0; n < EVENT_LISTS[i].length; n++) {
				AbstractEvent expectedEvent = EVENT_LISTS[i][n];
				AbstractEvent actualEvent = events.get(n);
				
				Assert.assertEquals(expectedEvent.getClass(), actualEvent.getClass());
				Assert.assertEquals(expectedEvent, actualEvent);
			}
			
			// check the buffer
			Buffer buf = env.getBuffer();
			if (buf == null) {
				Assert.assertTrue(BUFFER_SIZES[i] == 0);
			} else {
				Assert.assertEquals(BUFFER_SIZES[i], buf.size());
				for (int k = 0; k < BUFFER_SIZES[i]; k++) {
					Assert.assertEquals(BUFFER_CONTENT, buf.getMemorySegment().get(k));
				}
			}
			
			reader.reset();
		}
		
	}
	
	
	public  static final class TestEvent1 extends AbstractEvent {

		private long id;
		
		public TestEvent1() {}
		
		public TestEvent1(long id) {
			this.id = id;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
		}

		@Override
		public void read(DataInput in) throws IOException {
			id = in.readLong();
		}
		

		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == TestEvent1.class) {
				return ((TestEvent1) obj).id == this.id;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return ((int) id) ^ ((int) (id >>> 32));
		}
		
		@Override
		public String toString() {
			return "TestEvent1 (" + id + ")";
		}
	}
	
	public static final class TestEvent2 extends AbstractEvent {

		private long id;
		
		public TestEvent2() {}
		
		public TestEvent2(long id) {
			this.id = id;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(id);
		}

		@Override
		public void read(DataInput in) throws IOException {
			id = in.readLong();
		}
		

		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == TestEvent2.class) {
				return ((TestEvent2) obj).id == this.id;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return ((int) id) ^ ((int) (id >>> 32));
		}
		
		@Override
		public String toString() {
			return "TestEvent2 (" + id + ")";
		}
	}
	
	private static final class ChunkedWriteableChannel implements WritableByteChannel {
		
		private final WritableByteChannel delegate;
		
		private final Random rnd;
		
		private ChunkedWriteableChannel(WritableByteChannel delegate) {
			this.delegate = delegate;
			this.rnd = new Random();
		}

		@Override
		public boolean isOpen() {
			return this.delegate.isOpen();
		}

		@Override
		public void close() throws IOException {
			this.delegate.close();
		}

		@Override
		public int write(ByteBuffer src) throws IOException {
			final int available = src.remaining();
			final int oldLimit = src.limit();
			
			int toWrite = rnd.nextInt(available) + 1;
			toWrite = Math.min(Math.max(toWrite, 8), available);
			
			src.limit(src.position() + toWrite);
			
			int written = this.delegate.write(src);
			
			src.limit(oldLimit);
			
			return written;
		}
	}
	
	private static final class ChunkedReadableChannel implements ReadableByteChannel {
		
		private final ReadableByteChannel delegate;
		
		private final Random rnd;
		
		private ChunkedReadableChannel(ReadableByteChannel delegate) {
			this.delegate = delegate;
			this.rnd = new Random();
		}

		@Override
		public boolean isOpen() {
			return this.delegate.isOpen();
		}

		@Override
		public void close() throws IOException {
			this.delegate.close();
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			final int available = dst.remaining();
			final int oldLimit = dst.limit();
			
			int toRead = rnd.nextInt(available) + 1;
			toRead = Math.min(Math.max(toRead, 8), available);
			
			dst.limit(dst.position() + toRead);
			
			int read = this.delegate.read(dst);
			
			dst.limit(oldLimit);
			
			return read;
		}
	}
	
	private static final class OneForAllBroker implements BufferProviderBroker {

		private final TestBufferProvider provider;

		private OneForAllBroker(int sizeOfMemorySegments) {
			this.provider = new TestBufferProvider(sizeOfMemorySegments);
		}
		
		private OneForAllBroker(int sizeOfMemorySegments, float probabilityForNoBufferCurrently) {
			this.provider = new TestBufferProvider(sizeOfMemorySegments, probabilityForNoBufferCurrently);
		}
		
		@Override
		public BufferProvider getBufferProvider(JobID jobID, ChannelID sourceChannelID) {
			return this.provider;
		}
	}
}
