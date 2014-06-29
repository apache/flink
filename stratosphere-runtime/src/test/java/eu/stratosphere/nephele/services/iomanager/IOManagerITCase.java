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

package eu.stratosphere.nephele.services.iomanager;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;


/**
 * Integration test case for the I/O manager.
 *
 *
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
		memoryManager = new DefaultMemoryManager(NUMBER_OF_SEGMENTS * SEGMENT_SIZE, 1);
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
		
		Channel.ID[] ids = new Channel.ID[NUM_CHANNELS];
		BlockChannelWriter[] writers = new BlockChannelWriter[NUM_CHANNELS];
		BlockChannelReader[] readers = new BlockChannelReader[NUM_CHANNELS];
		ChannelWriterOutputView[] outs = new ChannelWriterOutputView[NUM_CHANNELS];
		ChannelReaderInputView[] ins = new ChannelReaderInputView[NUM_CHANNELS];
		
		int[] writingCounters = new int[NUM_CHANNELS];
		int[] readingCounters = new int[NUM_CHANNELS];
		
		// instantiate the channels and writers
		for (int i = 0; i < NUM_CHANNELS; i++)
		{
			ids[i] = this.ioManager.createChannel();
			writers[i] = this.ioManager.createBlockChannelWriter(ids[i]);
			
			List<MemorySegment> memSegs = this.memoryManager.allocatePages(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2);
			outs[i] = new ChannelWriterOutputView(writers[i], memSegs, this.memoryManager.getPageSize());
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
			val.write(outs[channel]);
		}
		LOG.info("Writing done, flushing contents...");
		
		// close all writers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			this.memoryManager.release(outs[i].close());
		}
		outs = null;
		writers = null;
		
		// instantiate the readers for sequential read
		LOG.info("Reading channels sequentially...");
		for (int i = 0; i < NUM_CHANNELS; i++)
		{
			List<MemorySegment> memSegs = this.memoryManager.allocatePages(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2);
			
			LOG.info("Reading channel " + (i+1) + "/" + NUM_CHANNELS + '.');
				
			final BlockChannelReader reader = this.ioManager.createBlockChannelReader(ids[i]);
			final ChannelReaderInputView in = new ChannelReaderInputView(reader, memSegs, false);
			int nextVal = 0;
			
			try {
				while (true) {
					val.read(in);
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
			} catch (EOFException eofex) {
				// expected
			}
			
			Assert.assertEquals("NUmber of written numbers differs from number of read numbers.", writingCounters[i], nextVal);
			
			this.memoryManager.release(in.close());
		}
		LOG.info("Sequential reading done.");
		
		// instantiate the readers
		LOG.info("Reading channels randomly...");
		for (int i = 0; i < NUM_CHANNELS; i++) {
			
			List<MemorySegment> memSegs = this.memoryManager.allocatePages(memOwner, rnd.nextInt(NUMBER_OF_SEGMENTS - 2) + 2);
				
			readers[i] = this.ioManager.createBlockChannelReader(ids[i]);
			ins[i] = new ChannelReaderInputView(readers[i], memSegs, false);
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
			
			while (true) {
				final int channel = skewedSample(rnd, NUM_CHANNELS - 1);
				if (ins[channel] != null) {
					try {
						val.read(ins[channel]);
						int intValue;
						try {
							intValue = Integer.parseInt(val.value);
						}
						catch (NumberFormatException nfex) {
							Assert.fail("Invalid value read from reader. Valid decimal number expected.");
							return;
						}
						
						Assert.assertEquals("Written and read values do not match.", readingCounters[channel]++, intValue);
						
						break;
					} catch (EOFException eofex) {
						this.memoryManager.release(ins[channel].close());
						ins[channel] = null;
					}
				}
			}
			
		}
		LOG.info("Random reading done.");
		
		// close all readers
		for (int i = 0; i < NUM_CHANNELS; i++) {
			if (ins[i] != null) {
				this.memoryManager.release(ins[i].close());
			}
			readers[i].closeAndDelete();
		}
		
		ins = null;
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
		public void read(DataInputView in) throws IOException {
			value = in.readUTF();
		}

		@Override
		public void write(DataOutputView out) throws IOException {
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
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Value other = (Value) obj;

			if (value == null) {
				if (other.value != null) {
					return false;
				}
			} else if (!value.equals(other.value)) {
				return false;
			}
			return true;
		}
	}

}
