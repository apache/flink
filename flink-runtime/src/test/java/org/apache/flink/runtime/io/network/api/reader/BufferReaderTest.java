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

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.io.network.api.reader.MockBufferReader.TestTaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Task.class)
public class BufferReaderTest {

	@Test
	public void testTaskEventNotification() throws IOException, InterruptedException {
		final MockBufferReader mockReader = new MockBufferReader()
				.readEvent().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);

		final BufferReader reader = mockReader.getMock();

		// Task event listener to be notified...
		reader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		consumeAndVerify(reader, 0);

		verifyListenerCalled(listener, 1);
	}

	@Test
	public void testGetNextBufferOrEvent() throws IOException, InterruptedException {

		final MockBufferReader mockReader = new MockBufferReader()
				.readBuffer().readBuffer().readEvent().readBuffer().readBuffer().readEvent().readBuffer().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);

		final BufferReader reader = mockReader.getMock();

		reader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		// Consume the reader
		consumeAndVerify(reader, 5);

		verifyListenerCalled(listener, 2);
	}

	@Test
	public void testIterativeGetNextBufferOrEvent() throws IOException, InterruptedException {

		final MockBufferReader mockReader = new MockBufferReader()
				.readBuffer().readBuffer().readEvent().readBuffer().readBuffer().readEvent().readBuffer().finishSuperstep()
				.readBuffer().readBuffer().readEvent().readBuffer().readBuffer().readEvent().readBuffer().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);

		final BufferReader reader = mockReader.getMock();

		// Set reader iterative
		reader.setIterativeReader();

		// Task event listener to be notified...
		reader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		// Consume the reader
		consumeAndVerify(reader, 10, 1);

		verifyListenerCalled(listener, 4);
	}

	@Test(expected = IOException.class)
	public void testExceptionEndOfSuperstepEventWithNonIterativeReader() throws IOException, InterruptedException {

		final MockBufferReader mockReader = new MockBufferReader().finishSuperstep();

		final BufferReader reader = mockReader.getMock();

		// Should throw Exception, because it's a non-iterative reader
		reader.getNextBufferBlocking();
	}

	// ------------------------------------------------------------------------

	static void verifyListenerCalled(EventListener<TaskEvent> mockListener, int expectedNumCalls) {
		verify(mockListener, times(expectedNumCalls)).onEvent(Matchers.any(TestTaskEvent.class));
	}

	static void consumeAndVerify(BufferReaderBase reader, int expectedNumReadBuffers) throws IOException, InterruptedException {
		consumeAndVerify(reader, expectedNumReadBuffers, 0);
	}

	static void consumeAndVerify(BufferReaderBase reader, int expectedNumReadBuffers, int expectedNumReadIterations) throws IOException, InterruptedException {
		int numReadBuffers = 0;
		int numIterations = 0;

		while (true) {
			Buffer buffer;
			while ((buffer = reader.getNextBufferBlocking()) != null) {
				buffer.recycle();

				numReadBuffers++;
			}

			if (reader.isFinished()) {
				break;
			}
			else if (reader.hasReachedEndOfSuperstep()) {
				reader.startNextSuperstep();

				numIterations++;
			}
			else {
				continue;
			}
		}

		assertEquals(expectedNumReadBuffers, numReadBuffers);
		assertEquals(expectedNumReadIterations, numIterations);
	}
}
