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
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest.DummyInvokable;


public class IOManagerTest
{	
	// ------------------------------------------------------------------------
	//                             Constants
	// ------------------------------------------------------------------------
	
	private static final long SEED = 649180756312423613L;

	private static final int VALUE_LENGTH = 118;

	private static final int NUMBER_OF_PAIRS = 1024 * 8 * 64;

	public static final int NUMBER_OF_SEGMENTS = 10; // 10

	public static final int SEGMENT_SIZE = 1024 * 1024; // 1M

	// ------------------------------------------------------------------------
	//                        Cross Test Fields
	// ------------------------------------------------------------------------
	
	private IOManager ioManager;

	private DefaultMemoryManager memoryManager;

	private Generator generator;

	// ------------------------------------------------------------------------
	//                          Mock Objects
	// ------------------------------------------------------------------------
	
//	@Mock
//	private RandomAccessFile failingFile1;
//	
//	@Mock
//	private RandomAccessFile failingFile2;
//	
//	@Mock
//	private FileChannel mockFailingFileChannel1;
//
//	@Mock
//	private FileChannel mockFailingFileChannel2;
//	
//	@Mock
//	private Channel.ID failingChannel1;
//	
//	@Mock
//	private Channel.ID failingChannel2;
	
	
	// ------------------------------------------------------------------------
	//                           Setup & Shutdown
	// ------------------------------------------------------------------------
	
	@Before
	public void beforeTest() {
		memoryManager = new DefaultMemoryManager(NUMBER_OF_SEGMENTS * SEGMENT_SIZE);
		ioManager = new IOManager();
		generator = new Generator(SEED, VALUE_LENGTH);
	}

	@After
	public void afterTest() {
		ioManager.shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", ioManager.isProperlyShutDown());
		
		Assert.assertTrue("Not all memory was returned to the memory manager in the test.", memoryManager.verifyEmpty());
		memoryManager.shutdown();
		memoryManager = null;
	}

	// ------------------------------------------------------------------------
	//                           Test Methods
	// ------------------------------------------------------------------------
	
	// ------------------------------------------------------------------------
	
//	/**
//	 * 
//	 */
//	@Test
//	public void testExceptionForwarding() throws Exception
//	{
//		final String failingVirtualFile1 = new String("virtual fail 1");
//		final String failingVirtualFile2 = new String("virtual fail 2");
//		
//		final AbstractInvokable memoryOwner = new DummyInvokable();
//		
//		final int NUM_MEM_SEGS = 4;
//		final int SIZE_MEM_SEGS = 4096;
//		
//		// set up the mocks
//		PowerMockito.when(failingChannel1.getPath()).thenReturn(failingVirtualFile1);
//		PowerMockito.when(failingChannel2.getPath()).thenReturn(failingVirtualFile2);
//			
//		PowerMockito.whenNew(RandomAccessFile.class).withParameterTypes(String.class, String.class).withArguments(Matchers.same(failingVirtualFile1), Matchers.any(String.class)).thenReturn(failingFile1);
//		PowerMockito.whenNew(RandomAccessFile.class).withParameterTypes(String.class, String.class).withArguments(Matchers.same(failingVirtualFile2), Matchers.any(String.class)).thenReturn(failingFile2);
//			
//		PowerMockito.when(failingFile1.getChannel()).thenReturn(mockFailingFileChannel1);
//		PowerMockito.when(failingFile2.getChannel()).thenReturn(mockFailingFileChannel2);
//		
//		try {
//			// create 4 readers out of which two will cause different exceptions
//			Channel.ID[] channels = new Channel.ID[4];
//			
//			channels[0] = ioManager.createChannel();
//			channels[2] = ioManager.createChannel();
//			channels[1] = failingChannel1;
//			channels[3] = failingChannel2;
//			
//			// create the writers for the channels
//			ChannelWriter[] writers = new ChannelWriter[channels.length];
//			for (int i = 0; i < writers.length; i++) {
//				writers[i] = ioManager.createChannelWriter(channels[i], memoryManager.allocate(memoryOwner, NUM_MEM_SEGS, SIZE_MEM_SEGS));
//			}
//			
//		}
//		catch (MemoryAllocationException maex) {
//			Assert.fail("Memory allocation exception happend during test.");
//		}
//		catch (IOException ioex) {
//			Assert.fail("IO exception happend during test.");
//		}
//	}

	/**
	 * Tests that the channel enumerator creates channels in the temporary files directory.
	 */
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
	public void channelReaderWriter() {
		Channel.ID channelID = ioManager.createChannel();

		// write generated key/value pairs to a channel
		int writtenCounter = writeToChannel(channelID);

		// read written pairs from the channel
		int readCounter = readFromChannel(channelID);

		Assert.assertEquals("counter equal", writtenCounter, readCounter);
	}

	private int writeToChannel(Channel.ID channelID) {
		try {
			// create the free memory segments to be used in the internal reader buffer flow
			Collection<MemorySegment> freeSegments = memoryManager.allocate(new DummyInvokable(), NUMBER_OF_SEGMENTS, SEGMENT_SIZE);
	
			// create the channel writer
			ChannelWriter channelWriter = ioManager.createChannelWriter(channelID, freeSegments);
	
			generator.reset();
	
			// get first pair and initialize written pairs and written buffers counter
			int writtenPairs = 0;
			while (writtenPairs < NUMBER_OF_PAIRS) {
				// writing was successful, get next pair and increment counter
				channelWriter.write(generator.next());
				writtenPairs++;
			}
	
			// close the writer and release the memory occupied by the buffers
			memoryManager.release(channelWriter.close());
	
			// return number of written buffers
			return writtenPairs;
		}
		catch (MemoryAllocationException maex) {
			Assert.fail("Memory for channel writers could not be allocated.");
			return 0;
		}
		catch (IOException ioex) {
			Assert.fail("IO error occurred while writing to the channel: " + ioex.getMessage());
			return 0;
		}
	}

	private int readFromChannel(Channel.ID channelID)  {
		try {
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
		catch (MemoryAllocationException maex) {
			Assert.fail("Memory for channel readers could not be allocated.");
			return 0;
		}
		catch (IOException ioex) {
			Assert.fail("IO error occurred while reading from the channel: " + ioex.getMessage());
			return 0;
		}
	}

	// ------------------------------------------------------------------------
	
	protected static class Value implements IOReadableWritable
	{
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

	protected static class Generator
	{
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
