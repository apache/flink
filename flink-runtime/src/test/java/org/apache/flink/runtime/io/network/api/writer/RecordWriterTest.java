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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.types.IntValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PrepareForTest(ResultPartitionWriter.class)
@RunWith(PowerMockRunner.class)
public class RecordWriterTest {

	// ---------------------------------------------------------------------------------------------
	// Resource release tests
	// ---------------------------------------------------------------------------------------------

	@Test
	public void testClearBuffersAfterEmit() throws Exception {
		final Buffer buffer = TestBufferFactory.createBuffer(32);

		BufferProvider bufferProvider = createBufferProvider(buffer);
		ResultPartitionWriter partitionWriter = createResultPartitionWriter(bufferProvider);

		RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

		// Emit single record, the buffer will not be written out.
		// Therefore, it needs to be cleared explicitly.
		recordWriter.emit(new IntValue(0));

		// Verify that a buffer is requested, but not written out.
		verify(bufferProvider, times(1)).requestBufferBlocking();
		verify(partitionWriter, never()).writeBuffer(any(Buffer.class), anyInt());

		recordWriter.clearBuffers();

		assertTrue("Buffer not recycled.", buffer.isRecycled());
	}

	@Test
	public void testClearBuffersAfterExceptionInFlushWritePartition() throws Exception {
		// Size of buffer ensures that a single record will fill the buffer.
		final Buffer buffer = TestBufferFactory.createBuffer(4);

		BufferProvider bufferProvider = createBufferProvider(buffer);
		ResultPartitionWriter partitionWriter = createResultPartitionWriter(bufferProvider);

		doThrow(new IOException("Expected test exception"))
				.when(partitionWriter).writeBuffer(eq(buffer), eq(0));

		RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

		try {
			// Emit single record, the buffer will not be written out,
			// because of the Exception. Therefore, it needs to be cleared
			// explicitly.
			recordWriter.emit(new IntValue(0));

			fail("Did not throw expected Exception. This means that the record "
					+ "writer did not request a buffer as expected.");
		}
		catch (IOException expected) {
		}

		// Verify that a buffer is requested, but not written out due to the Exception.
		verify(bufferProvider, times(1)).requestBufferBlocking();
		verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());

		recordWriter.clearBuffers();

		assertTrue("Buffer not recycled.", buffer.isRecycled());

	}

	@Test
	public void testClearBuffersAfterExceptionInEmitWritePartition() throws Exception {
		// Size of buffer ensures that a single record will NOT fill the buffer.
		final Buffer buffer = TestBufferFactory.createBuffer(32);

		BufferProvider bufferProvider = createBufferProvider(buffer);
		ResultPartitionWriter partitionWriter = createResultPartitionWriter(bufferProvider);

		doThrow(new IOException("Expected test exception"))
				.when(partitionWriter).writeBuffer(eq(buffer), eq(0));

		RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

		try {
			recordWriter.emit(new IntValue(0));

			// Verify that a buffer is requested, but not written out.
			verify(bufferProvider, times(1)).requestBufferBlocking();
			verify(partitionWriter, never()).writeBuffer(any(Buffer.class), anyInt());

			// Now flush the record.
			recordWriter.flush();

			fail("Did not throw expected Exception. This means that the record "
					+ "writer did not request a buffer as expected.");
		}
		catch (IOException expected) {
		}

		// Flushing the buffer tried to write out the buffer.
		verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());

		recordWriter.clearBuffers();

		assertTrue("Buffer not recycled.", buffer.isRecycled());
	}

	/**
	 * Tests a fix for FLINK-2089.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-2089">FLINK-2089</a>
	 */
	@Test
	public void testClearBuffersAfterInterruptDuringBlockingBufferRequest() throws Exception {
		ExecutorService executor = null;

		try {
			executor = Executors.newSingleThreadExecutor();

			final CountDownLatch sync = new CountDownLatch(2);

			final Buffer buffer = spy(TestBufferFactory.createBuffer(4));

			// Return buffer for first request, but block for all following requests.
			Answer<Buffer> request = new Answer<Buffer>() {
				@Override
				public Buffer answer(InvocationOnMock invocation) throws Throwable {
					sync.countDown();

					if (sync.getCount() == 1) {
						return buffer;
					}

					final Object o = new Object();
					synchronized (o) {
						while (true) {
							o.wait();
						}
					}
				}
			};

			BufferProvider bufferProvider = mock(BufferProvider.class);
			when(bufferProvider.requestBufferBlocking()).thenAnswer(request);

			ResultPartitionWriter partitionWriter = createResultPartitionWriter(bufferProvider);

			final RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

			Future<?> result = executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					IntValue val = new IntValue(0);

					try {
						recordWriter.emit(val);
						recordWriter.flush();

						recordWriter.emit(val);
					}
					catch (InterruptedException e) {
						recordWriter.clearBuffers();
					}

					return null;
				}
			});

			sync.await();

			// Interrupt the Thread.
			//
			// The second emit call requests a new buffer and blocks the thread.
			// When interrupting the thread at this point, clearing the buffers
			// should not recycle any buffer.
			result.cancel(true);

			recordWriter.clearBuffers();

			// Verify that buffer have been requested, but only one has been written out.
			verify(bufferProvider, times(2)).requestBufferBlocking();
			verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());

			// Verify that the written out buffer has only been recycled once
			// (by the partition writer).
			assertTrue("Buffer not recycled.", buffer.isRecycled());
			verify(buffer, times(1)).recycle();
		}
		finally {
			if (executor != null) {
				executor.shutdown();
			}
		}
	}

	@Test
	public void testSerializerClearedAfterClearBuffers() throws Exception {

		final Buffer buffer = TestBufferFactory.createBuffer(16);

		ResultPartitionWriter partitionWriter = createResultPartitionWriter(
				createBufferProvider(buffer));

		RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

		// Fill a buffer, but don't write it out.
		recordWriter.emit(new IntValue(0));
		verify(partitionWriter, never()).writeBuffer(any(Buffer.class), anyInt());

		// Clear all buffers.
		recordWriter.clearBuffers();

		// This should not throw an Exception iff the serializer state
		// has been cleared as expected.
		recordWriter.flush();
	}

	// ---------------------------------------------------------------------------------------------
	// Helpers
	// ---------------------------------------------------------------------------------------------

	private BufferProvider createBufferProvider(Buffer... buffers)
			throws IOException, InterruptedException {

		BufferProvider bufferProvider = mock(BufferProvider.class);

		for (int i = 0; i < buffers.length; i++) {
			when(bufferProvider.requestBufferBlocking()).thenReturn(buffers[i]);
		}

		return bufferProvider;
	}

	private ResultPartitionWriter createResultPartitionWriter(BufferProvider bufferProvider)
			throws IOException {

		ResultPartitionWriter partitionWriter = mock(ResultPartitionWriter.class);
		when(partitionWriter.getBufferProvider()).thenReturn(checkNotNull(bufferProvider));
		when(partitionWriter.getNumberOfOutputChannels()).thenReturn(1);

		// Recycle each written buffer.
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				((Buffer) invocation.getArguments()[0]).recycle();

				return null;
			}
		}).when(partitionWriter).writeBuffer(any(Buffer.class), anyInt());

		return partitionWriter;
	}
}
