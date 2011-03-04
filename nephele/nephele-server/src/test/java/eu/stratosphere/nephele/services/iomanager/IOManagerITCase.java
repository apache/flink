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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;


/**
 * Integration test case for the I/O manager.
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IOManagerITCase {
	
	private static final Log LOG = LogFactory.getLog(IOManagerITCase.class);
	
	private static final long SEED = 649180756312423613L;

	private static final int NUMBER_OF_SEGMENTS = 10; // 10

	private static final int SEGMENT_SIZE = 1024 * 1024; // 1M
	
	private final int NUM_CHANNELS = 29;
	
	private final int NUMBERS_TO_BE_WRITTEN = NUM_CHANNELS * 1000000;

	private IOManager ioManager;

	private DefaultMemoryManager memoryManager;


	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(NUMBER_OF_SEGMENTS * SEGMENT_SIZE);
		ioManager = new IOManager();
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
	
	/**
	 * This test instantiates multiple channels and writes to them in parallel and re-reads the data in 
	 * parallel. It is designed to check the ability of the IO manager to correctly handle multiple threads.
	 */
	@Test
	public void parallelChannelsTest() throws Exception
	{
		LOG.info("Starting parallel channels test.");
		
		final Random rnd = new Random(SEED);
		final AbstractInvokable memOwner = new DefaultMemoryManagerTest.DummyInvokable();
		
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
		int nextLogCount = 0;
		float nextLogFraction = 0.0f;
		
		LOG.info("Writing to channels...");
		for (int i = 0; i < NUMBERS_TO_BE_WRITTEN; i++) {
			
			if (i == nextLogCount) {
				LOG.info("... " + (int) (nextLogFraction * 100) + "% done.");
				nextLogFraction += 0.05;
				nextLogCount = (int) (nextLogFraction * NUMBERS_TO_BE_WRITTEN);
			}
			
			int channel = skewedSample(rnd, NUM_CHANNELS - 1);
			
			val.value = String.valueOf(writingCounters[channel]++);
			writers[channel].write(val);
		}
		LOG.info("Writing done, flushing contents...");
		
		// close all writers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			memoryManager.release(writers[i].close());
		}
		writers = null;
		
		// instantiate the readers for sequential read
		LOG.info("Reading channels sequentially...");
		for (int i = 0; i < NUM_CHANNELS; i++) {
			final int segmentSize = rnd.nextInt(maxSegmentSize - minSegmentSize) + minSegmentSize;
			Collection<MemorySegment> memSegs= memoryManager.allocate(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2, segmentSize);
			
			LOG.info("Reading channel " + i + "/" + NUM_CHANNELS + '.');
				
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
		LOG.info("Sequential reading done.");
		
		// instantiate the readers
		LOG.info("Reading channels randomly...");
		for (int i = 0; i < NUM_CHANNELS; i++) {
			
			final int segmentSize = rnd.nextInt(maxSegmentSize - minSegmentSize) + minSegmentSize;
			Collection<MemorySegment> memSegs = memoryManager.allocate(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2, segmentSize);
				
			readers[i] = ioManager.createChannelReader(ids[i], memSegs, true);
		}
		
		nextLogCount = 0;
		nextLogFraction = 0.0f;
		
		// read a lot of values in a mixed order from the channels
		for (int i = 0; i < NUMBERS_TO_BE_WRITTEN; i++) {
			
			if (i == nextLogCount) {
				LOG.info("... " + (int) (nextLogFraction * 100) + "% done.");
				nextLogFraction += 0.05;
				nextLogCount = (int) (nextLogFraction * NUMBERS_TO_BE_WRITTEN);
			}
			
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
		LOG.info("Random reading done.");
		
		// close all readers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			memoryManager.release(readers[i].close());
		}
		readers = null;
		
		// check that files are deleted
		for (int i = 0; i < NUM_CHANNELS; i++) {
			File f = new File(ids[i].getPath());
			Assert.assertFalse("Channel file has not been deleted.", f.exists());
		}
	}
	
	private static final int skewedSample(Random rnd, int max) {
		double uniform = rnd.nextDouble();
		double var = Math.pow(uniform, 8.0);
		double pareto = 0.2 / var;
		
		int val = (int) pareto;
		return val > max ? val % max : val;
	}
	
	
	// ------------------------------------------------------------------------
	
	protected static class Value implements IOReadableWritable {

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

}
