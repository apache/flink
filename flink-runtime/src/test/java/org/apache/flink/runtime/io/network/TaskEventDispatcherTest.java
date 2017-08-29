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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Basic tests for {@link TaskEventDispatcher}.
 */
public class TaskEventDispatcherTest extends TestLogger {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void registerPartitionTwice() throws Exception {
		ResultPartitionID partitionId = new ResultPartitionID();
		TaskEventDispatcher ted = new TaskEventDispatcher();
		ted.registerPartition(partitionId);

		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("already registered at task event dispatcher");

		ted.registerPartition(partitionId);
	}

	@Test
	public void subscribeToEventNotRegistered() throws Exception {
		TaskEventDispatcher ted = new TaskEventDispatcher();

		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage("not registered at task event dispatcher");

		//noinspection unchecked
		ted.subscribeToEvent(new ResultPartitionID(), mock(EventListener.class), TaskEvent.class);
	}

	/**
	 * Tests {@link TaskEventDispatcher#publish(ResultPartitionID, TaskEvent)} and {@link TaskEventDispatcher#subscribeToEvent(ResultPartitionID, EventListener, Class)} methods.
	 */
	@Test
	public void publishSubscribe() throws Exception {
		ResultPartitionID partitionId1 = new ResultPartitionID();
		ResultPartitionID partitionId2 = new ResultPartitionID();
		TaskEventDispatcher ted = new TaskEventDispatcher();

		AllWorkersDoneEvent event1 = new AllWorkersDoneEvent();
		assertFalse(ted.publish(partitionId1, event1));

		ted.registerPartition(partitionId1);
		ted.registerPartition(partitionId2);

		// no event listener subscribed yet, but the event is forwarded to a TaskEventHandler
		assertTrue(ted.publish(partitionId1, event1));

		//noinspection unchecked
		EventListener<TaskEvent> eventListener1a = mock(EventListener.class);
		//noinspection unchecked
		EventListener<TaskEvent> eventListener1b = mock(EventListener.class);
		//noinspection unchecked
		EventListener<TaskEvent> eventListener2 = mock(EventListener.class);
		//noinspection unchecked
		EventListener<TaskEvent> eventListener3 = mock(EventListener.class);
		ted.subscribeToEvent(partitionId1, eventListener1a, AllWorkersDoneEvent.class);
		ted.subscribeToEvent(partitionId2, eventListener1b, AllWorkersDoneEvent.class);
		ted.subscribeToEvent(partitionId1, eventListener2, TaskEvent.class);
		ted.subscribeToEvent(partitionId1, eventListener3, TerminationEvent.class);

		assertTrue(ted.publish(partitionId1, event1));
		verify(eventListener1a, times(1)).onEvent(event1);
		verify(eventListener1b, times(0)).onEvent(any());
		verify(eventListener2, times(0)).onEvent(any());
		verify(eventListener3, times(0)).onEvent(any());

		// publish another event, verify that only the right subscriber is called
		TerminationEvent event2 = new TerminationEvent();
		assertTrue(ted.publish(partitionId1, event2));
		verify(eventListener1a, times(1)).onEvent(event1);
		verify(eventListener1b, times(0)).onEvent(any());
		verify(eventListener2, times(0)).onEvent(any());
		verify(eventListener3, times(1)).onEvent(event2);
	}

	@Test
	public void unregisterPartition() throws Exception {
		ResultPartitionID partitionId1 = new ResultPartitionID();
		ResultPartitionID partitionId2 = new ResultPartitionID();
		TaskEventDispatcher ted = new TaskEventDispatcher();

		AllWorkersDoneEvent event = new AllWorkersDoneEvent();
		assertFalse(ted.publish(partitionId1, event));

		ted.registerPartition(partitionId1);
		ted.registerPartition(partitionId2);

		//noinspection unchecked
		EventListener<TaskEvent> eventListener1a = mock(EventListener.class);
		//noinspection unchecked
		EventListener<TaskEvent> eventListener1b = mock(EventListener.class);
		//noinspection unchecked
		EventListener<TaskEvent> eventListener2 = mock(EventListener.class);
		ted.subscribeToEvent(partitionId1, eventListener1a, AllWorkersDoneEvent.class);
		ted.subscribeToEvent(partitionId2, eventListener1b, AllWorkersDoneEvent.class);
		ted.subscribeToEvent(partitionId1, eventListener2, AllWorkersDoneEvent.class);

		ted.unregisterPartition(partitionId2);

		// publis something for partitionId1 triggering all according listeners
		assertTrue(ted.publish(partitionId1, event));
		verify(eventListener1a, times(1)).onEvent(event);
		verify(eventListener1b, times(0)).onEvent(any());
		verify(eventListener2, times(1)).onEvent(event);

		// now publish something for partitionId2 which should not trigger any listeners
		assertFalse(ted.publish(partitionId2, event));
		verify(eventListener1a, times(1)).onEvent(event);
		verify(eventListener1b, times(0)).onEvent(any());
		verify(eventListener2, times(1)).onEvent(event);
	}

	@Test
	public void clearAll() throws Exception {
		ResultPartitionID partitionId = new ResultPartitionID();
		TaskEventDispatcher ted = new TaskEventDispatcher();
		ted.registerPartition(partitionId);

		//noinspection unchecked
		EventListener<TaskEvent> eventListener1 = mock(EventListener.class);
		ted.subscribeToEvent(partitionId, eventListener1, AllWorkersDoneEvent.class);

		ted.clearAll();

		assertFalse(ted.publish(partitionId, new AllWorkersDoneEvent()));
		verify(eventListener1, times(0)).onEvent(any());
	}
}
