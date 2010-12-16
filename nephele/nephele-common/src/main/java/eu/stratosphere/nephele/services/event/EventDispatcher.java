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

package eu.stratosphere.nephele.services.event;

import java.util.HashSet;
import java.util.Set;

public class EventDispatcher<T extends Event> {
	public final Set<EventHandler<? extends T>> handlers;

	public EventDispatcher() {
		handlers = new HashSet<EventHandler<? extends T>>();
	}

	public <U extends T> void register(EventHandler<U> handler) {
	}

	public <U extends T> void unregister(EventHandler<U> handler) {
		handlers.remove(handler);
	}

	protected void dispatch(T event) {
	}
}
