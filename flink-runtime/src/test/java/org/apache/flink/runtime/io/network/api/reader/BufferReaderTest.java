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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.TestSingleInputGate;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Task.class)
@SuppressWarnings("unchecked")
public class BufferReaderTest {

	@Test
	public void testGetNextBufferOrEvent() throws IOException, InterruptedException {

		final TestSingleInputGate inputGate = new TestSingleInputGate(1)
				.readBuffer().readBuffer().readEvent()
				.readBuffer().readBuffer().readEvent()
				.readBuffer().readEndOfPartitionEvent();

		final BufferReader reader = new BufferReader(inputGate.getInputGate());

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);
		reader.registerTaskEventListener(listener, TestTaskEvent.class);

		int numReadBuffers = 0;
		while ((reader.getNextBuffer()) != null) {
			numReadBuffers++;
		}

		assertEquals(5, numReadBuffers);
		verify(listener, times(2)).onEvent(any(TaskEvent.class));
	}

	@Test
	public void testIterativeGetNextBufferOrEvent() throws IOException, InterruptedException {

		final TestSingleInputGate inputGate = new TestSingleInputGate(1)
				.readBuffer().readBuffer().readEvent()
				.readBuffer().readBuffer().readEvent()
				.readBuffer().readEndOfSuperstepEvent()
				.readBuffer().readBuffer().readEvent()
				.readBuffer().readBuffer().readEvent()
				.readBuffer().readEndOfPartitionEvent();

		final BufferReader reader = new BufferReader(inputGate.getInputGate());

		// Set reader iterative
		reader.setIterativeReader();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);
		// Task event listener to be notified...
		reader.registerTaskEventListener(listener, TestTaskEvent.class);

		int numReadBuffers = 0;
		int numEndOfSuperstepEvents = 0;

		while (true) {
			Buffer buffer = reader.getNextBuffer();

			if (buffer != null) {
				numReadBuffers++;
			}
			else if (reader.hasReachedEndOfSuperstep()) {
				reader.startNextSuperstep();

				numEndOfSuperstepEvents++;
			}
			else if (reader.isFinished()) {
				break;
			}
		}

		assertEquals(10, numReadBuffers);
		assertEquals(1, numEndOfSuperstepEvents);

		verify(listener, times(4)).onEvent(any(TaskEvent.class));
	}
}
