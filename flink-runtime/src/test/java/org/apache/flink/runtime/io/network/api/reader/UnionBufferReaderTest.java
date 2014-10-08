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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.runtime.io.network.BufferOrEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.reader.BufferReaderTest.TestTaskEvent;
import org.apache.flink.runtime.io.network.api.reader.BufferReaderTest.TestTaskEventListener;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnionBufferReaderTest {

	private NetworkBufferPool buffers;

	private BufferPool bufferPool;

	@Before
	public void setupNetworkBufferPool() {
		buffers = new NetworkBufferPool(1024, 16 * 1024);
		bufferPool = buffers.createBufferPool(1024, true);
	}

	@After
	public void verifyAllBuffersReturned() {
		bufferPool.destroy();
		assertEquals(buffers.getNumMemorySegments(), buffers.getNumAvailableMemorySegments());
	}

	@Test
	public void testTaskEventNotification() throws IOException, InterruptedException {
		BufferReader reader1, reader2;

		{
			final InputChannel mockInputChannel = mock(InputChannel.class);

			reader1 = new BufferReader(new InputChannel[]{mockInputChannel}, mock(BufferPool.class));

			when(mockInputChannel.getNextBufferOrEvent())
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenAnswer(new Answer<BufferOrEvent>() {
						@Override
						public BufferOrEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
							when(mockInputChannel.isFinished()).thenReturn(true);

							return new BufferOrEvent(new EndOfPartitionEvent());
						}
					});
		}

		{
			final InputChannel mockInputChannel = mock(InputChannel.class);

			reader2 = new BufferReader(new InputChannel[]{mockInputChannel}, mock(BufferPool.class));

			when(mockInputChannel.getNextBufferOrEvent())
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenAnswer(new Answer<BufferOrEvent>() {
						@Override
						public BufferOrEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
							when(mockInputChannel.isFinished()).thenReturn(true);

							return new BufferOrEvent(new EndOfPartitionEvent());
						}
					});
		}

		UnionBufferReader unionReader = new UnionBufferReader(reader1, reader2);

		reader1.onAvailableInputChannel(reader1.getInputChannel(0));
		reader1.onAvailableInputChannel(reader1.getInputChannel(0));

		reader2.onAvailableInputChannel(reader2.getInputChannel(0));
		reader2.onAvailableInputChannel(reader2.getInputChannel(0));

		// --------------------------------------------------------------------
		// Subscribe task event listener
		// --------------------------------------------------------------------
		TestTaskEventListener testTaskEventListener = new TestTaskEventListener();
		unionReader.subscribeToTaskEvent(testTaskEventListener, TestTaskEvent.class);

		assertNull(unionReader.getNextBuffer());
		assertEquals(2, testTaskEventListener.getNumTaskEvents());
	}

	@Test
	public void testGetNextBufferOrEvent() throws IOException, InterruptedException {
		BufferReader reader1, reader2;

		{
			final InputChannel mockInputChannel = mock(InputChannel.class);

			reader1 = new BufferReader(new InputChannel[]{mockInputChannel}, bufferPool);

			when(mockInputChannel.getNextBufferOrEvent())
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenAnswer(new Answer<BufferOrEvent>() {
						@Override
						public BufferOrEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
							when(mockInputChannel.isFinished()).thenReturn(true);

							return new BufferOrEvent(new EndOfPartitionEvent());
						}
					});
		}

		{
			final InputChannel mockInputChannel = mock(InputChannel.class);

			reader2 = new BufferReader(new InputChannel[]{mockInputChannel}, bufferPool);

			when(mockInputChannel.getNextBufferOrEvent())
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenAnswer(new Answer<BufferOrEvent>() {
						@Override
						public BufferOrEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
							when(mockInputChannel.isFinished()).thenReturn(true);

							return new BufferOrEvent(new EndOfPartitionEvent());
						}
					});
		}

		UnionBufferReader unionReader = new UnionBufferReader(reader1, reader2);

		for (int i = 0; i < 8; i++) {
			reader1.onAvailableInputChannel(reader1.getInputChannel(0));
			reader2.onAvailableInputChannel(reader2.getInputChannel(0));
		}

		// --------------------------------------------------------------------
		// Subscribe task event listener
		// --------------------------------------------------------------------
		TestTaskEventListener testTaskEventListener = new TestTaskEventListener();
		unionReader.subscribeToTaskEvent(testTaskEventListener, TestTaskEvent.class);

		// --------------------------------------------------------------------
		// Verify all buffers are read
		// --------------------------------------------------------------------
		int numReadBuffers = 0;

		Buffer buffer;
		while ((buffer = unionReader.getNextBuffer()) != null) {
			buffer.recycle();

			numReadBuffers++;
		}

		assertEquals(10, numReadBuffers);
		assertEquals(4, testTaskEventListener.getNumTaskEvents());
	}

	@Test
	public void testIterativeGetNextBufferOrEvent() throws IOException, InterruptedException {
		BufferReader reader1, reader2;

		{
			final InputChannel mockInputChannel = mock(InputChannel.class);

			reader1 = new BufferReader(new InputChannel[]{mockInputChannel}, bufferPool);

			when(mockInputChannel.getNextBufferOrEvent())
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(EndOfSuperstepEvent.INSTANCE))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenAnswer(new Answer<BufferOrEvent>() {
						@Override
						public BufferOrEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
							when(mockInputChannel.isFinished()).thenReturn(true);

							return new BufferOrEvent(new EndOfPartitionEvent());
						}
					});
		}

		{
			final InputChannel mockInputChannel = mock(InputChannel.class);

			reader2 = new BufferReader(new InputChannel[]{mockInputChannel}, bufferPool);

			when(mockInputChannel.getNextBufferOrEvent())
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(EndOfSuperstepEvent.INSTANCE))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenReturn(new BufferOrEvent(new TestTaskEvent()))
					.thenReturn(new BufferOrEvent(bufferPool.requestBuffer().waitForBuffer().getBuffer()))
					.thenAnswer(new Answer<BufferOrEvent>() {
						@Override
						public BufferOrEvent answer(InvocationOnMock invocationOnMock) throws Throwable {
							when(mockInputChannel.isFinished()).thenReturn(true);

							return new BufferOrEvent(new EndOfPartitionEvent());
						}
					});
		}

		UnionBufferReader unionReader = new UnionBufferReader(reader1, reader2);
		unionReader.setIterativeReader();

		for (int i = 0; i < 16; i++) {
			reader1.onAvailableInputChannel(reader1.getInputChannel(0));
		}

		for (int i = 0; i < 16; i++) {
			reader2.onAvailableInputChannel(reader2.getInputChannel(0));
		}

		// --------------------------------------------------------------------
		// Subscribe task event listener
		// --------------------------------------------------------------------
		TestTaskEventListener testTaskEventListener = new TestTaskEventListener();
		unionReader.subscribeToTaskEvent(testTaskEventListener, TestTaskEvent.class);

		// --------------------------------------------------------------------
		// Verify all buffers are read
		// --------------------------------------------------------------------
		int numReadBuffers = 0;
		int numIterations = 0;

		while (true) {
			Buffer buffer;
			while ((buffer = unionReader.getNextBuffer()) != null) {
				buffer.recycle();

				numReadBuffers++;
			}

			numIterations++;

			if (unionReader.isFinished()) {
				break;
			}
		}

		assertEquals(2, numIterations);
		assertEquals(20, numReadBuffers);
		assertEquals(8, testTaskEventListener.getNumTaskEvents());
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionEndOfSuperstepEventWithNonIterativeReader() throws IOException, InterruptedException {
		BufferReader reader = new BufferReader(new InputChannel[0], mock(BufferPool.class));

		InputChannel mockInputChannel = mock(InputChannel.class);

		reader.onAvailableInputChannel(mockInputChannel);

		when(mockInputChannel.getNextBufferOrEvent()).thenReturn(new BufferOrEvent(EndOfSuperstepEvent.INSTANCE));

		reader.getNextBuffer();
	}
}
