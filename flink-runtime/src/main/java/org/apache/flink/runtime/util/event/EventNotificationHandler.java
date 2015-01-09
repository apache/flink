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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * The event handler manages {@link EventListener} instances and allows to
 * to publish events to them.
 */
public class EventNotificationHandler<T> {

	// Listeners for each event type
	private final Multimap<Class<? extends T>, EventListener<T>> listeners = HashMultimap.create();

	public void subscribe(EventListener<T> listener, Class<? extends T> eventType) {
		synchronized (listeners) {
			listeners.put(eventType, listener);
		}
	}

	public void unsubscribe(EventListener<T> listener, Class<? extends T> eventType) {
		synchronized (listeners) {
			listeners.remove(eventType, listener);
		}
	}

	/**
	 * Publishes the event to all subscribed {@link EventListener} objects.
	 *
	 * @param event The event to publish.
	 */
	public void publish(T event) {
		synchronized (listeners) {
			for (EventListener<T> listener : listeners.get((Class<? extends T>) event.getClass())) {
				listener.onEvent(event);
			}
		}
	}
}
