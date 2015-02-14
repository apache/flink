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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class AsynchronousFileIOChannelsTest {

	@Test
	public void testClosingWaits() {
		IOManagerAsync ioMan = new IOManagerAsync();
		try {
			
			final int NUM_BLOCKS = 100;
			final MemorySegment seg = new MemorySegment(new byte[32 * 1024]);
			
			final AtomicInteger callbackCounter = new AtomicInteger();
			final AtomicBoolean exceptionOccurred = new AtomicBoolean();
			
			final RequestDoneCallback<MemorySegment> callback = new RequestDoneCallback<MemorySegment>() {
				
				@Override
				public void requestSuccessful(MemorySegment buffer) {
					// we do the non safe variant. the callbacks should come in order from
					// the same thread, so it should always work
					callbackCounter.set(callbackCounter.get() + 1);
					
					if (buffer != seg) {
						exceptionOccurred.set(true);
					}
				}
				
				@Override
				public void requestFailed(MemorySegment buffer, IOException e) {
					exceptionOccurred.set(true);
				}
			};
			
			BlockChannelWriterWithCallback writer = ioMan.createBlockChannelWriter(ioMan.createChannel(), callback);
			try {
				for (int i = 0; i < NUM_BLOCKS; i++) {
					writer.writeBlock(seg);
				}
				
				writer.close();
				
				assertEquals(NUM_BLOCKS, callbackCounter.get());
				assertFalse(exceptionOccurred.get());
			}
			finally {
				writer.closeAndDelete();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			ioMan.shutdown();
		}
	}
	
	@Test
	public void testExceptionForwardsToClose() {
		IOManagerAsync ioMan = new IOManagerAsync();
		try {
			testExceptionForwardsToClose(ioMan, 100, 1);
			testExceptionForwardsToClose(ioMan, 100, 50);
			testExceptionForwardsToClose(ioMan, 100, 100);
		} finally {
			ioMan.shutdown();
		}
	}
	
	private void testExceptionForwardsToClose(IOManagerAsync ioMan, final int numBlocks, final int failingBlock) {
		try {
			MemorySegment seg = new MemorySegment(new byte[32 * 1024]);
			FileIOChannel.ID channelId = ioMan.createChannel();
			
			BlockChannelWriterWithCallback writer = new AsynchronousBlockWriterWithCallback(channelId, 
					ioMan.getWriteRequestQueue(channelId), new NoOpCallback()) {
				
				private int numBlocks;
				
				@Override
				public void writeBlock(MemorySegment segment) throws IOException {
					numBlocks++;
					
					if (numBlocks == failingBlock) {
						this.requestsNotReturned.incrementAndGet();
						this.requestQueue.add(new FailingWriteRequest(this, segment));
					} else {
						super.writeBlock(segment);
					}
				}
			};
			
			try {
				for (int i = 0; i < numBlocks; i++) {
					writer.writeBlock(seg);
				}
				
				writer.close();
				fail("did not forward exception");
			}
			catch (IOException e) {
				// expected
			}
			finally {
				try {
					writer.closeAndDelete();
				} catch (Throwable t) {}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static class NoOpCallback implements RequestDoneCallback<MemorySegment> {

		@Override
		public void requestSuccessful(MemorySegment buffer) {}

		@Override
		public void requestFailed(MemorySegment buffer, IOException e) {}
	}
	
	private static class FailingWriteRequest implements WriteRequest {
		
		private final AsynchronousFileIOChannel<MemorySegment, WriteRequest> channel;
		
		private final MemorySegment segment;
		
		protected FailingWriteRequest(AsynchronousFileIOChannel<MemorySegment, WriteRequest> targetChannel, MemorySegment segment) {
			this.channel = targetChannel;
			this.segment = segment;
		}

		@Override
		public void write() throws IOException {
			throw new IOException();
		}

		@Override
		public void requestDone(IOException ioex) {
			this.channel.handleProcessedBuffer(this.segment, ioex);
		}
	} 
}