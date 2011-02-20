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

package eu.stratosphere.nephele.services.iomanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.ChannelReader;
import eu.stratosphere.nephele.services.iomanager.ChannelWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest.DummyInvokable;

public class IOManagerTest {
	private static final long SEED = 649180756312423613L;

	private static final int VALUE_LENGTH = 118;

	private static final int NUMBER_OF_PAIRS = 1024 * 8 * 64;

	public static final int NUMBER_OF_SEGMENTS = 10; // 10MB

	public static final int SEGMENT_SIZE = 1024 * 1024; // 1M + 64 bytes

	private IOManager ioManager;

	private DefaultMemoryManager memoryManager;

	private Generator generator;


	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(NUMBER_OF_SEGMENTS * SEGMENT_SIZE);
		ioManager = new IOManager();
		generator = new Generator(SEED, VALUE_LENGTH);
	}

	@After
	public void afterTest() throws Exception {
		ioManager.shutdown();
		
		Assert.assertTrue("IO Manager has not properly shut down.", ioManager.isProperlyShutDown());
		
		if (!memoryManager.verifyEmpty()) {
			Assert.fail("Not all memory was returned to the memory manager in the test.");
		}
		
		memoryManager.shutdown();
		memoryManager = null;
	}

	@Test
	public void channelEnumerator() {
		Channel.Enumerator enumerator = ioManager.createChannelEnumerator();

		for (int i = 0; i < 10; i++) {
			enumerator.next();
		}
	}

	@Test
	public void channelReaderWriter() throws MemoryAllocationException, IOException {
		Channel.ID channelID = ioManager.createChannel();

		// write generated key/value pairs to a channel
		int writtenCounter = writeToChannel(channelID);

		// read written pairs from the channel
		int readCounter = readFromChannel(channelID);

		Assert.assertEquals("counter equal", writtenCounter, readCounter);
	}

	private int writeToChannel(Channel.ID channelID) throws IOException, MemoryAllocationException{
		// create the free memory segments to be used in the internal reader buffer flow
		Collection<MemorySegment> freeSegments = memoryManager.allocate(new DummyInvokable(), NUMBER_OF_SEGMENTS, SEGMENT_SIZE);

		// create the channel writer
		ChannelWriter channelWriter = ioManager.createChannelWriter(channelID, freeSegments);

		generator.reset();

		// get first pair and initialize written pairs and written buffers counter
		int writtenPairs = 0;
		while (writtenPairs < NUMBER_OF_PAIRS) {
			// writing was successfull, get next pair and increment counter
			channelWriter.write(generator.next());
			writtenPairs++;
		}

		// close the writer and release the memory occupied by the buffers
		memoryManager.release(channelWriter.close());

		// return number of written buffers
		return writtenPairs;
	}

	private int readFromChannel(Channel.ID channelID) throws IOException, MemoryAllocationException {
		// create the free memory segments to be used in the internal reader buffer flow
		Collection<MemorySegment> freeSegments = memoryManager.allocate(new DummyInvokable(), NUMBER_OF_SEGMENTS, SEGMENT_SIZE);

		// create the channel reader
		ChannelReader channelReader = ioManager.createChannelReader(channelID, freeSegments);

		generator.reset();

		Value value = new Value();
		int readCounter = 0;
		while (channelReader.read(value)) {
			Assert.assertEquals("Pairs don't match", generator.next(), value);
			readCounter++;
		}

		// close the reader and release the memory occupied by the buffers
		memoryManager.release(channelReader.close());

		return readCounter;
	}

	private class Value implements IOReadableWritable {

		String value;

		public Value() {
		}

		public Value(String val) {
			this.value = val;
		}

		@Override
		public void read(DataInput in) throws IOException {
			value = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.value);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((value == null) ? 0 : value.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Value other = (Value) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}

		private IOManagerTest getOuterType() {
			return IOManagerTest.this;
		}

	}

	private class Generator {

		private long seed;

		private Random rand;

		private byte[] value;

		public Generator(long seed, int valueLength) {
			this.seed = seed;

			this.rand = new Random(this.seed);
			this.value = new byte[valueLength];
		}

		public void reset() {
			this.rand.setSeed(this.seed);
		}

		public Value next() {

			rand.nextBytes(this.value);
			return new Value(this.value.toString());
		}

	}
}
