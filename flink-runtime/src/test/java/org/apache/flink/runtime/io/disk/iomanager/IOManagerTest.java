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


package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.DefaultMemoryManagerTest.DummyInvokable;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class IOManagerTest {
	
	// ------------------------------------------------------------------------
	//                        Cross Test Fields
	// ------------------------------------------------------------------------
	
	private IOManager ioManager;

	private DefaultMemoryManager memoryManager;
	
	// ------------------------------------------------------------------------
	//                           Setup & Shutdown
	// ------------------------------------------------------------------------
	
	@Before
	public void beforeTest() {
		this.memoryManager = new DefaultMemoryManager(32 * 1024 * 1024, 1);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() {
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
		
		FileIOChannel.Enumerator enumerator = ioManager.createChannelEnumerator();

		for (int i = 0; i < 10; i++) {
			FileIOChannel.ID id = enumerator.next();
			
			File path = new File(id.getPath());
			Assert.assertTrue("Channel IDs must name an absolute path.", path.isAbsolute());
			Assert.assertFalse("Channel IDs must name a file, not a directory.", path.isDirectory());
			Assert.assertTrue("Path is not in the temp directory.", tempPath.equals(path.getParentFile()));
		}
	}

	// ------------------------------------------------------------------------
	
	@Test
	public void channelReadWriteOneSegment() {
		final int NUM_IOS = 1111;
		
		try {
			final FileIOChannel.ID channelID = this.ioManager.createChannel();
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
	public void channelReadWriteMultipleSegments() {
		final int NUM_IOS = 1111;
		final int NUM_SEGS = 16;
		
		try {
			final List<MemorySegment> memSegs = this.memoryManager.allocatePages(new DummyInvokable(), NUM_SEGS);
			final FileIOChannel.ID channelID = this.ioManager.createChannel();
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
	
	final class FailingSegmentReadRequest implements ReadRequest {
		
		private final AsynchronousFileIOChannel<MemorySegment, ReadRequest> channel;
		
		private final MemorySegment segment;
		
		protected FailingSegmentReadRequest(AsynchronousFileIOChannel<MemorySegment, ReadRequest> targetChannel, MemorySegment segment) {
			this.channel = targetChannel;
			this.segment = segment;
		}


		@Override
		public void read() throws IOException {
			throw new TestIOException();
		}


		@Override
		public void requestDone(IOException ioex) {
			this.channel.handleProcessedBuffer(this.segment, ioex);
		}
	}

	//--------------------------------------------------------------------------------------------

	/**
	 * Special write request that writes an entire memory segment to the block writer.
	 */
	final class FailingSegmentWriteRequest implements WriteRequest {
		
		private final AsynchronousFileIOChannel<MemorySegment, WriteRequest> channel;
		
		private final MemorySegment segment;
		
		protected FailingSegmentWriteRequest(AsynchronousFileIOChannel<MemorySegment, WriteRequest> targetChannel, MemorySegment segment) {
			this.channel = targetChannel;
			this.segment = segment;
		}

		@Override
		public void write() throws IOException {
			throw new TestIOException();
		}

		@Override
		public void requestDone(IOException ioex) {
			this.channel.handleProcessedBuffer(this.segment, ioex);
		}
	}
	
	
	final class TestIOException extends IOException {
		private static final long serialVersionUID = -814705441998024472L;
	}
}
