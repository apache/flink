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


package org.apache.flink.runtime.util.event;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.runtime.event.task.StringTaskEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This class contains unit tests for the {@link TaskEventHandler}.
 * 
 */
public class TaskEventHandlerTest {
	/**
	 * A test implementation of an {@link EventListener}.
	 * 
	 */
	private static class TestEventListener implements EventListener<TaskEvent> {

		/**
		 * The event that was last received by this event listener.
		 */
		private TaskEvent receivedEvent = null;

		/**
		 * {@inheritDoc}
		 * @param event
		 */
		@Override
		public void onEvent(TaskEvent event) {

			this.receivedEvent = event;
		}

		/**
		 * Returns the event which was last received by this event listener. If no event
		 * has been received so far the return value is <code>null</code>.
		 * 
		 * @return the event which was last received, possibly <code>null</code>
		 */
		public TaskEvent getLastReceivedEvent() {

			return this.receivedEvent;
		}
	}

	/**
	 * Tests the publish/subscribe mechanisms implemented in the {@link TaskEventHandler}.
	 */
	@Test
	public void testEventNotificationManager() {

		final TaskEventHandler evm = new TaskEventHandler();
		final TestEventListener listener = new TestEventListener();

		evm.subscribe(listener, StringTaskEvent.class);

		final StringTaskEvent stringTaskEvent1 = new StringTaskEvent("Test 1");

		evm.publish(stringTaskEvent1);
		evm.publish(new IntegerTaskEvent(5));

		assertNotNull(listener.getLastReceivedEvent());
		StringTaskEvent receivedStringEvent = (StringTaskEvent) listener.getLastReceivedEvent();
		assertEquals(stringTaskEvent1, receivedStringEvent);
	}
}
