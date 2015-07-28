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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.types.LongValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * This test uses the PowerMockRunner runner to work around the fact that the 
 * {@link ResultPartitionWriter} class is final.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultPartitionWriter.class)
public class StreamRecordWriterTest {

	/**
	 * Verifies that exceptions during flush from the output flush thread are
	 * recognized in the writer.
	 */
	@Test
	public void testPropagateAsyncFlushError() {
		FailingWriter<LongValue> testWriter = null;
		try {
			ResultPartitionWriter mockResultPartitionWriter = getMockWriter(5);
			
			// test writer that flushes every 5ms and fails after 3 flushes
			testWriter = new FailingWriter<LongValue>(mockResultPartitionWriter,
					new RoundRobinChannelSelector<LongValue>(), 5, 3);
			
			try {
				long deadline = System.currentTimeMillis() + 20000; // in max 20 seconds (conservative)
				long l = 0L;
				
				while (System.currentTimeMillis() < deadline) {
					testWriter.emit(new LongValue(l++));
				}
				
				fail("This should have failed with an exception");
			}
			catch (IOException e) {
				assertNotNull(e.getCause());
				assertTrue(e.getCause().getMessage().contains("Test Exception"));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (testWriter != null) {
				try {
					testWriter.close();
				}
				catch (IOException e) {
					// ignore in tests
				}
			}
		}
	}
	
	private static ResultPartitionWriter getMockWriter(int numPartitions) throws Exception {
		BufferProvider mockProvider = mock(BufferProvider.class);
		when(mockProvider.requestBufferBlocking()).thenAnswer(new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocation) {
				return new Buffer(new MemorySegment(new byte[4096]), DummyBufferRecycler.INSTANCE);
			}
		});
		
		ResultPartitionWriter mockWriter = mock(ResultPartitionWriter.class);
		when(mockWriter.getBufferProvider()).thenReturn(mockProvider);
		when(mockWriter.getNumberOfOutputChannels()).thenReturn(numPartitions);
		
		
		return mockWriter;
	}
	
	
	// ------------------------------------------------------------------------
	
	private static class FailingWriter<T extends IOReadableWritable> extends StreamRecordWriter<T> {
		
		private int flushesBeforeException;
		
		private FailingWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector,
								long timeout, int flushesBeforeException) {
			super(writer, channelSelector, timeout);
			this.flushesBeforeException = flushesBeforeException;
		}

		@Override
		public void flush() throws IOException {
			if (flushesBeforeException-- <= 0) {
				throw new IOException("Test Exception");
			}
			super.flush();
		}
	}
}
