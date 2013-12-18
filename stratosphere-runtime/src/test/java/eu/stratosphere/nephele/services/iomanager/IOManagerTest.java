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

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.DefaultMemoryManagerTest.DummyInvokable;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;

public class IOManagerTest
{
	// ------------------------------------------------------------------------
	//                        Cross Test Fields
	// ------------------------------------------------------------------------
	
	private IOManager ioManager;

	private DefaultMemoryManager memoryManager;
	
	// ------------------------------------------------------------------------
	//                           Setup & Shutdown
	// ------------------------------------------------------------------------
	
	@Before
	public void beforeTest()
	{
		this.memoryManager = new DefaultMemoryManager(32 * 1024 * 1024);
		this.ioManager = new IOManager();
	}

	@After
	public void afterTest()
	{
		this.ioManager.shutdown();
		Assert.assertTrue("IO Manager has not properly shut down.", ioManager.isProperlyShutDown());
		
		Assert.assertTrue("Not all memory was returned to the memory manager in the test.", this.memoryManager.verifyEmpty());
		this.memoryManager.shutdown();
		this.memoryManager = null;
	}

	// ------------------------------------------------------------------------
	//                           Test Methods
	// ------------------------------------------------------------------------
	
	// ------------------------------------------------------------------------

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
	public void channelReadWriteOneSegment()
	{
		final int NUM_IOS = 1111;
		
		try {
			final Channel.ID channelID = this.ioManager.createChannel();
			final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channelID);
			
			MemorySegment memSeg = this.memoryManager.allocatePages(new DummyInvokable(), 1).get(0);
			
			for (int i = 0; i < NUM_IOS; i++) {
				for (int pos = 0; pos < memSeg.size(); pos += 4) {
					memSeg.putInt(pos, i);
				}
				
				writer.writeBlock(memSeg);
				memSeg = writer.getNextReturnedSegment();
			}
			
			writer.close();
			
			final BlockChannelReader reader = this.ioManager.createBlockChannelReader(channelID);
			for (int i = 0; i < NUM_IOS; i++) {
				reader.readBlock(memSeg);
				memSeg = reader.getNextReturnedSegment();
				
				for (int pos = 0; pos < memSeg.size(); pos += 4) {
					if (memSeg.getInt(pos) != i) {
						Assert.fail("Read memory segment contains invalid data.");
					}
				}
			}
			
			reader.closeAndDelete();
			
			this.memoryManager.release(memSeg);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("TEst encountered an exception: " + ex.getMessage());
		}
	}
	
	@Test
	public void channelReadWriteMultipleSegments()
	{
		final int NUM_IOS = 1111;
		final int NUM_SEGS = 16;
		
		try {
			final List<MemorySegment> memSegs = this.memoryManager.allocatePages(new DummyInvokable(), NUM_SEGS);
			final Channel.ID channelID = this.ioManager.createChannel();
			final BlockChannelWriter writer = this.ioManager.createBlockChannelWriter(channelID);
			
			for (int i = 0; i < NUM_IOS; i++) {
				final MemorySegment memSeg = memSegs.isEmpty() ? writer.getNextReturnedSegment() : memSegs.remove(0);
				
				for (int pos = 0; pos < memSeg.size(); pos += 4) {
					memSeg.putInt(pos, i);
				}
				
				writer.writeBlock(memSeg);
			}
			writer.close();
			
			// get back the memory
			while (memSegs.size() < NUM_SEGS) {
				memSegs.add(writer.getNextReturnedSegment());
			}
			
			final BlockChannelReader reader = this.ioManager.createBlockChannelReader(channelID);
			while(!memSegs.isEmpty()) {
				reader.readBlock(memSegs.remove(0));
			}
			
			for (int i = 0; i < NUM_IOS; i++) {
				final MemorySegment memSeg = reader.getNextReturnedSegment();
				
				for (int pos = 0; pos < memSeg.size(); pos += 4) {
					if (memSeg.getInt(pos) != i) {
						Assert.fail("Read memory segment contains invalid data.");
					}
				}
				reader.readBlock(memSeg);
			}
			
			reader.closeAndDelete();
			
			// get back the memory
			while (memSegs.size() < NUM_SEGS) {
				memSegs.add(reader.getNextReturnedSegment());
			}
			
			this.memoryManager.release(memSegs);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("TEst encountered an exception: " + ex.getMessage());
		}
	}

	// ============================================================================================
	
	final class FailingSegmentReadRequest implements ReadRequest
	{
		private final BlockChannelAccess<ReadRequest, ?> channel;
		
		private final MemorySegment segment;
		
		protected FailingSegmentReadRequest(BlockChannelAccess<ReadRequest, ?> targetChannel, MemorySegment segment)
		{
			this.channel = targetChannel;
			this.segment = segment;
		}


		@Override
		public void read() throws IOException
		{
			throw new TestIOException();
		}


		@Override
		public void requestDone(IOException ioex)
		{
			this.channel.handleProcessedBuffer(this.segment, ioex);
		}
	}

	//--------------------------------------------------------------------------------------------

	/**
	 * Special write request that writes an entire memory segment to the block writer.
	 */
	final class FailingSegmentWriteRequest implements WriteRequest
	{
		private final BlockChannelAccess<WriteRequest, ?> channel;
		
		private final MemorySegment segment;
		
		protected FailingSegmentWriteRequest(BlockChannelAccess<WriteRequest, ?> targetChannel, MemorySegment segment)
		{
			this.channel = targetChannel;
			this.segment = segment;
		}


		@Override
		public void write() throws IOException
		{
			throw new TestIOException();
		}


		@Override
		public void requestDone(IOException ioex)
		{
			this.channel.handleProcessedBuffer(this.segment, ioex);
		}
	}
	
	
	final class TestIOException extends IOException
	{
		private static final long serialVersionUID = -814705441998024472L;
	}
}
