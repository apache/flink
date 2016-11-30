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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.LongValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
			} catch (IOException e) {
				assertNotNull(e.getCause());
				assertTrue(e.getCause().getMessage().contains("Test Exception"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			if (testWriter != null) {
				testWriter.close();
			}
		}
	}

	/**
	 * Tests that the output flusher only calls try flush.
	 */
	@Test
	public void testOutputFlusherUsesTryFlush() throws Exception {
		BufferProvider bufferProvider = new TestInfiniteBufferProvider();
		final CountDownLatch latch = new CountDownLatch(10);

		ResultPartition partition = mock(ResultPartition.class);
		when(partition.getNumberOfSubpartitions()).thenReturn(1);
		when(partition.addBufferIfCapacityAvailable(any(Buffer.class), anyInt())).thenAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				latch.countDown();
				return null;
			}
		});
		when(partition.getBufferProvider()).thenReturn(bufferProvider);

		StreamRecordWriter<ByteValue> writer = new StreamRecordWriter<>(
			new ResultPartitionWriter(partition),
			new RoundRobinChannelSelector<ByteValue>(),
			2);

		ByteValue value = new ByteValue();
		value.setValue((byte) 12);

		long deadline = System.currentTimeMillis() + 30_000;
		while (latch.getCount() != 0 && System.currentTimeMillis() <= deadline) {
			writer.emit(value);
			Thread.sleep(10);
		}

		assertEquals(0, latch.getCount());

		// There is a very low probability that the main test thread
		// emits so many records that it actually writes out the
		// buffer itself (at which point add is called). If this test
		// is flakey on CI infra, rethink this line.
		verify(partition, never()).add(any(Buffer.class), anyInt(), anyBoolean());
		verify(partition, atLeast(10)).addBufferIfCapacityAvailable(any(Buffer.class), eq(0));
	}

	private static ResultPartitionWriter getMockWriter(int numPartitions) throws Exception {
		BufferProvider mockProvider = mock(BufferProvider.class);
		when(mockProvider.requestBufferBlocking()).thenAnswer(new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocation) {
				return new Buffer(
					MemorySegmentFactory.allocateUnpooledSegment(4096),
					FreeingBufferRecycler.INSTANCE);
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
		public void tryFlush() throws IOException {
			if (flushesBeforeException-- <= 0) {
				throw new IOException("Test Exception");
			}
			super.tryFlush();
		}

		@Override
		public void flush() throws IOException, InterruptedException {
			if (flushesBeforeException-- <= 0) {
				throw new IOException("Test Exception");
			}
			super.flush();
		}
	}
}
