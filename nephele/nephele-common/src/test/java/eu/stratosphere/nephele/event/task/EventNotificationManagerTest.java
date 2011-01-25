/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.event.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * This class contains unit tests for the {@link EventNotificationManager}.
 * 
 * @author warneke
 */
public class EventNotificationManagerTest {

	/**
	 * A test implementation of an {@link EventListener}.
	 * 
	 * @author warneke
	 */
	private static class TestEventListener implements EventListener {

		/**
		 * The event that was last received by this event listener.
		 */
		private AbstractTaskEvent receivedEvent = null;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void eventOccurred(AbstractTaskEvent event) {

			this.receivedEvent = event;
		}

		/**
		 * Returns the event which was last received by this event listener. If no event
		 * has been received so far the return value is <code>null</code>.
		 * 
		 * @return the event which was last received, possibly <code>null</code>
		 */
		public AbstractTaskEvent getLastReceivedEvent() {

			return this.receivedEvent;
		}
	}

	/**
	 * Tests the publish/subscribe mechanisms implemented in the {@link EventNotificationManager}.
	 */
	@Test
	public void testEventNotificationManager() {

		final EventNotificationManager evm = new EventNotificationManager();
		final TestEventListener listener = new TestEventListener();

		evm.subscribeToEvent(listener, StringTaskEvent.class);

		final StringTaskEvent stringTaskEvent1 = new StringTaskEvent("Test 1");
		final StringTaskEvent stringTaskEvent2 = new StringTaskEvent("Test 2");

		evm.deliverEvent(stringTaskEvent1);
		evm.deliverEvent(new IntegerTaskEvent(5));

		assertNotNull(listener.getLastReceivedEvent());
		StringTaskEvent receivedStringEvent = (StringTaskEvent) listener.getLastReceivedEvent();
		assertEquals(stringTaskEvent1, receivedStringEvent);

		evm.unsubscribeFromEvent(listener, StringTaskEvent.class);

		evm.deliverEvent(stringTaskEvent2);

		assertNotNull(listener.getLastReceivedEvent());
		receivedStringEvent = (StringTaskEvent) listener.getLastReceivedEvent();
		assertEquals(stringTaskEvent1, receivedStringEvent);
	}
}
