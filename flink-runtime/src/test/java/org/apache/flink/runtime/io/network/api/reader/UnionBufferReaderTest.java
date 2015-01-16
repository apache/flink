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
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.api.reader.BufferReaderTest.consumeAndVerify;
import static org.apache.flink.runtime.io.network.api.reader.BufferReaderTest.verifyListenerCalled;
import static org.apache.flink.runtime.io.network.api.reader.MockBufferReader.TestTaskEvent;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Task.class)
public class UnionBufferReaderTest {

	@Test
	public void testTaskEventNotifications() throws IOException, InterruptedException {
		final MockBufferReader reader1 = new MockBufferReader();
		final MockBufferReader reader2 = new MockBufferReader();

		final UnionBufferReader unionReader = new UnionBufferReader(reader1.getMock(), reader2.getMock());

		reader1.readEvent().finish();
		reader2.readEvent().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);

		unionReader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		consumeAndVerify(unionReader, 0);

		verifyListenerCalled(listener, 2);
	}

	@Test
	public void testGetNextBufferOrEvent() throws IOException, InterruptedException {
		final MockBufferReader reader1 = new MockBufferReader();
		final MockBufferReader reader2 = new MockBufferReader();

		final UnionBufferReader unionReader = new UnionBufferReader(reader1.getMock(), reader2.getMock());

		reader1.readBuffer().readBuffer().readEvent().readBuffer().readBuffer().readEvent().readBuffer().finish();

		reader2.readBuffer().readBuffer().readEvent().readBuffer().readBuffer().readEvent().readBuffer().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);

		unionReader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		// Consume the reader
		consumeAndVerify(unionReader, 10);

		verifyListenerCalled(listener, 4);
	}

	@Test
	public void testIterativeGetNextBufferOrEvent() throws IOException, InterruptedException {
		final MockBufferReader reader1 = new MockBufferReader();
		final MockBufferReader reader2 = new MockBufferReader();

		final UnionBufferReader unionReader = new UnionBufferReader(reader1.getMock(), reader2.getMock());

		unionReader.setIterativeReader();

		reader1.readBuffer().readBuffer().readEvent()
				.readBuffer().readBuffer().readEvent()
				.readBuffer().finishSuperstep().readBuffer().readBuffer()
				.readEvent().readBuffer().readBuffer()
				.readEvent().readBuffer().finish();

		reader2.readBuffer().readBuffer().readEvent()
				.readBuffer().readBuffer().readEvent()
				.readBuffer().finishSuperstep().readBuffer().readBuffer()
				.readEvent().readBuffer().readBuffer()
				.readEvent().readBuffer().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);
		unionReader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		// Consume the reader
		consumeAndVerify(unionReader, 20, 1);

		verifyListenerCalled(listener, 8);
	}

	@Test
	public void testGetNextBufferUnionOfUnionReader() throws Exception {
		final MockBufferReader reader1 = new MockBufferReader();
		final MockBufferReader reader2 = new MockBufferReader();

		final UnionBufferReader unionReader = new UnionBufferReader(reader1.getMock(), reader2.getMock());

		final MockBufferReader reader3 = new MockBufferReader();

		final UnionBufferReader unionUnionReader = new UnionBufferReader(unionReader, reader3.getMock());

		reader1.readBuffer().readBuffer().readBuffer().readEvent().readEvent().readBuffer().finish();

		reader2.readEvent().readBuffer().readBuffer().readEvent().readBuffer().finish();

		reader3.readBuffer().readBuffer().readEvent().readEvent().finish();

		// Task event listener to be notified...
		final EventListener<TaskEvent> listener = mock(EventListener.class);
		unionUnionReader.subscribeToTaskEvent(listener, TestTaskEvent.class);

		// Consume the reader
		consumeAndVerify(unionUnionReader, 9);

		verifyListenerCalled(listener, 6);
	}
}
