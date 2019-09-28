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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;

import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the event handling behaviour.
 */
public class AbstractReaderTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testTaskEvent() throws Exception {
		final AbstractReader reader = new MockReader(createInputGate(1));

		final EventListener<TaskEvent> listener1 = mock(EventListener.class);
		final EventListener<TaskEvent> listener2 = mock(EventListener.class);
		final EventListener<TaskEvent> listener3 = mock(EventListener.class);

		reader.registerTaskEventListener(listener1, TestTaskEvent1.class);
		reader.registerTaskEventListener(listener2, TestTaskEvent2.class);
		reader.registerTaskEventListener(listener3, TaskEvent.class);

		reader.handleEvent(new TestTaskEvent1()); // for listener1 only
		reader.handleEvent(new TestTaskEvent2()); // for listener2 only

		verify(listener1, times(1)).onEvent(Matchers.any(TaskEvent.class));
		verify(listener2, times(1)).onEvent(Matchers.any(TaskEvent.class));
		verify(listener3, times(0)).onEvent(Matchers.any(TaskEvent.class));
	}

	@Test
	public void testEndOfPartitionEvent() throws Exception {
		final AbstractReader reader = new MockReader(createInputGate(1));

		assertTrue(reader.handleEvent(EndOfPartitionEvent.INSTANCE));
	}

	/**
	 * Ensure that all end of superstep event related methods throw an Exception when used with a
	 * non-iterative reader.
	 */
	@Test
	public void testExceptionsNonIterativeReader() throws Exception {

		final AbstractReader reader = new MockReader(createInputGate(4));

		// Non-iterative reader cannot reach end of superstep
		assertFalse(reader.hasReachedEndOfSuperstep());

		try {
			reader.startNextSuperstep();

			fail("Did not throw expected exception when starting next superstep with non-iterative reader.");
		}
		catch (Throwable t) {
			// All good, expected exception.
		}

		try {
			reader.handleEvent(EndOfSuperstepEvent.INSTANCE);

			fail("Did not throw expected exception when handling end of superstep event with non-iterative reader.");
		}
		catch (Throwable t) {
			// All good, expected exception.
		}
	}

	@Test
	public void testEndOfSuperstepEventLogic() throws IOException {

		final int numberOfInputChannels = 4;
		final AbstractReader reader = new MockReader(createInputGate(numberOfInputChannels));

		reader.setIterativeReader();

		try {
			// The first superstep does not need not to be explicitly started
			reader.startNextSuperstep();

			fail("Did not throw expected exception when starting next superstep before receiving all end of superstep events.");
		}
		catch (Throwable t) {
			// All good, expected exception.
		}

		EndOfSuperstepEvent eos = EndOfSuperstepEvent.INSTANCE;

		// One end of superstep event for each input channel. The superstep finishes with the last
		// received event.
		for (int i = 0; i < numberOfInputChannels - 1; i++) {
			assertFalse(reader.handleEvent(eos));
			assertFalse(reader.hasReachedEndOfSuperstep());
		}

		assertTrue(reader.handleEvent(eos));
		assertTrue(reader.hasReachedEndOfSuperstep());

		try {
			// Verify exception, when receiving too many end of superstep events.
			reader.handleEvent(eos);

			fail("Did not throw expected exception when receiving too many end of superstep events.");
		}
		catch (Throwable t) {
			// All good, expected exception.
		}

		// Start next superstep.
		reader.startNextSuperstep();
		assertFalse(reader.hasReachedEndOfSuperstep());
	}

	private InputGate createInputGate(int numberOfInputChannels) {
		final InputGate inputGate = mock(InputGate.class);
		when(inputGate.getNumberOfInputChannels()).thenReturn(numberOfInputChannels);

		return inputGate;
	}

	// ------------------------------------------------------------------------

	private static class TestTaskEvent1 extends TaskEvent {

		@Override
		public void write(DataOutputView out) throws IOException {
		}

		@Override
		public void read(DataInputView in) throws IOException {
		}
	}

	private static class TestTaskEvent2 extends TaskEvent {

		@Override
		public void write(DataOutputView out) throws IOException {
		}

		@Override
		public void read(DataInputView in) throws IOException {
		}
	}

	private static class MockReader extends AbstractReader {

		protected MockReader(InputGate inputGate) {
			super(inputGate);
		}
	}
}
