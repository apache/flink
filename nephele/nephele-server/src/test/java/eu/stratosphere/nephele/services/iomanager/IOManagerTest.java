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
import java.io.File;
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
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest.DummyInvokable;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class IOManagerTest {
	
	private static final long SEED = 649180756312423613L;

	private static final int VALUE_LENGTH = 118;

	private static final int NUMBER_OF_PAIRS = 1024 * 8 * 64;

	public static final int NUMBER_OF_SEGMENTS = 10; // 10

	public static final int SEGMENT_SIZE = 1024 * 1024; // 1M

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
		
		Assert.assertTrue("Not all memory was returned to the memory manager in the test.", memoryManager.verifyEmpty());
		memoryManager.shutdown();
		memoryManager = null;
	}

	// ------------------------------------------------------------------------
	
	@Test
	public void channelEnumerator() {
		File tempPath = new File(System.getProperty("java.io.tmpdir")); 
		
		Channel.Enumerator enumerator = ioManager.createChannelEnumerator();

		for (int i = 0; i < 10; i++) {
			Channel.ID id = enumerator.next();
			
			File path = new File(id.getPath());
			Assert.assertTrue("Channel IDs must name an absolute path.", path.isAbsolute());
			Assert.assertFalse("Channel IDs must name a file, not a directory.", path.isDirectory());
			Assert.assertTrue("Path is not in the temp directory.", tempPath.equals(path.getParentFile()));
		}
	}

	// ------------------------------------------------------------------------
	
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
		ChannelReader channelReader = ioManager.createChannelReader(channelID, freeSegments, true);

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

	// ------------------------------------------------------------------------
	
	/**
	 * This test instantiates multiple channels and writes to them in parallel and re-reads the data in 
	 * parallel. It is designed to check the ability of the IO manager to correctly handle multiple threads.
	 */
	@Test
	public void parallelChannelsTest() throws Exception
	{
		final Random rnd = new Random(236976457234657898l);
		final AbstractInvokable memOwner = new DefaultMemoryManagerTest.DummyInvokable();
		
		final int NUM_CHANNELS = 29;
		final int NUMBERS_TO_BE_WRITTEN = NUM_CHANNELS * 100000;
		
		final int minSegmentSize = 4 * 1024;
		final int maxSegmentSize = SEGMENT_SIZE / NUM_CHANNELS;
		
		Channel.ID[] ids = new Channel.ID[NUM_CHANNELS];
		Writer[] writers = new Writer[NUM_CHANNELS];
		Reader[] readers = new Reader[NUM_CHANNELS];
		
		int[] writingCounters = new int[NUM_CHANNELS];
		int[] readingCounters = new int[NUM_CHANNELS];
		
		// instantiate the channels and writers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			ids[i] = this.ioManager.createChannel();
			
			final int segmentSize = rnd.nextInt(maxSegmentSize - minSegmentSize) + minSegmentSize;
			Collection<MemorySegment> memSegs= memoryManager.allocate(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2, segmentSize);
				
			writers[i] = ioManager.createChannelWriter(ids[i], memSegs);
		}
		
		
		Value val = new Value();
		
		// write a lot of values unevenly distributed over the channels
		for (int i = 0; i < NUMBERS_TO_BE_WRITTEN; i++) {
			int channel = skewedSample(rnd, NUM_CHANNELS - 1);
			
			val.value = String.valueOf(writingCounters[channel]++);
			writers[channel].write(val);
		}
		
		// close all writers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			memoryManager.release(writers[i].close());
		}
		writers = null;
		
		// instantiate the readers for sequential read
		for (int i = 0; i < NUM_CHANNELS; i++) {
			final int segmentSize = rnd.nextInt(maxSegmentSize - minSegmentSize) + minSegmentSize;
			Collection<MemorySegment> memSegs= memoryManager.allocate(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2, segmentSize);
				
			Reader reader = ioManager.createChannelReader(ids[i], memSegs, false);
			int nextVal = 0;
			
			while (reader.read(val)) {
				int intValue = 0;
				try {
					intValue = Integer.parseInt(val.value);
				}
				catch (NumberFormatException nfex) {
					Assert.fail("Invalid value read from reader. Valid decimal number expected.");
				}
				Assert.assertEquals("Written and read values do not match during sequential read.", nextVal, intValue);
				nextVal++;
			}
			
			Assert.assertEquals("NUmber of written numbers differs from number of read numbers.", writingCounters[i], nextVal);
			
			memoryManager.release(reader.close());
		}
		
		
		
		// instantiate the readers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			
			final int segmentSize = rnd.nextInt(maxSegmentSize - minSegmentSize) + minSegmentSize;
			Collection<MemorySegment> memSegs = memoryManager.allocate(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2, segmentSize);
				
			readers[i] = ioManager.createChannelReader(ids[i], memSegs, true);
		}
		
		// read a lot of values in a mixed order from the channels
		for (int i = 0; i < NUMBERS_TO_BE_WRITTEN; i++) {
			int channel = skewedSample(rnd, NUM_CHANNELS - 1);
			
			if (!readers[channel].read(val)) {
				continue;
			}
			
			int intValue = 0;
			try {
				intValue = Integer.parseInt(val.value);
			}
			catch (NumberFormatException nfex) {
				Assert.fail("Invalid value read from reader. Valid decimal number expected.");
			}
			
			Assert.assertEquals("Written and read values do not match.", readingCounters[channel]++, intValue);
		}
		
		// close all readers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			memoryManager.release(readers[i].close());
		}
		readers = null;
	}
	
	
	private static final int skewedSample(Random rnd, int max) {
		double uniform = rnd.nextDouble();
		double var = Math.pow(uniform, 8.0);
		double pareto = 0.2 / var;
		
		int val = (int) pareto;
		return val > max ? val % max : val;
	}
	
	
	// ------------------------------------------------------------------------
	
	private static class Value implements IOReadableWritable {

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

			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
			return true;
		}
	}

	private static class Generator {

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
