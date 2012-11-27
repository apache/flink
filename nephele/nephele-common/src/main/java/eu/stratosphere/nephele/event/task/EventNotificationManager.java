/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The event notification manager manages the subscription of {@link EventListener} objects to
 * particular event types. Moreover, it handles the dispatching of the events.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class EventNotificationManager {

	/**
	 * Stores the subscriptions for the individual event types.
	 */
	private final Map<Class<? extends AbstractTaskEvent>, List<EventListener>> subscriptions = new HashMap<Class<? extends AbstractTaskEvent>, List<EventListener>>();

	/**
	 * Subscribes the given event listener object to the specified event type.
	 * 
	 * @param eventListener
	 *        the {@link EventListener} object to create the subscription for
	 * @param eventType
	 *        the event type the given listener object wants to be notified about
	 */
	public void subscribeToEvent(final EventListener eventListener, final Class<? extends AbstractTaskEvent> eventType) {

		synchronized (this.subscriptions) {

			List<EventListener> subscribers = this.subscriptions.get(eventType);
			if (subscribers == null) {
				subscribers = new ArrayList<EventListener>();
				this.subscriptions.put(eventType, subscribers);
			}
			subscribers.add(eventListener);
		}

	}

	/**
	 * Removes a subscription of an {@link EventListener} object for the given event type.
	 * 
	 * @param eventListener
	 *        the event listener to remove the subscription for
	 * @param eventType
	 *        the event type to remove the subscription for
	 */
	public void unsubscribeFromEvent(final EventListener eventListener, final Class<? extends AbstractEvent> eventType) {

		synchronized (this.subscriptions) {

			List<EventListener> subscribers = this.subscriptions.get(eventType);
			if (subscribers == null) {
				return;
			}
			subscribers.remove(eventListener);
			if (subscribers.isEmpty()) {
				this.subscriptions.remove(eventType);
			}
		}

	}

	/**
	 * Delivers a event to all of the registered subscribers.
	 * 
	 * @param event
	 *        the event to deliver
	 */
	public void deliverEvent(final AbstractTaskEvent event) {

		synchronized (this.subscriptions) {

			List<EventListener> subscribers = this.subscriptions.get(event.getClass());
			if (subscribers == null) {
				return;
			}

			final Iterator<EventListener> it = subscribers.iterator();
			while (it.hasNext()) {
				it.next().eventOccurred(event);
			}
		}
	}

}
