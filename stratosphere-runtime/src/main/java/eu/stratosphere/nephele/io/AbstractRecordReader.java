/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.event.task.EventNotificationManager;

/**
 * This is an abstract base class for a record reader, either dealing with mutable or immutable records,
 * and dealing with reads from single gates (single end points) or multiple gates (union).
 */
public abstract class AbstractRecordReader implements ReaderBase {
	
	
	private final EventNotificationManager eventHandler = new EventNotificationManager();
	
	private int numEventsUntilEndOfSuperstep = -1;
	
	private int endOfSuperstepEventsCount;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Subscribes the listener object to receive events of the given type.
	 * 
	 * @param eventListener
	 *        the listener object to register
	 * @param eventType
	 *        the type of event to register the listener for
	 */
	@Override
	public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
		this.eventHandler.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * Removes the subscription for events of the given type for the listener object.
	 * 
	 * @param eventListener The listener object to cancel the subscription for.
	 * @param eventType The type of the event to cancel the subscription for.
	 */
	@Override
	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
		this.eventHandler.unsubscribeFromEvent(eventListener, eventType);
	}
	
	
	protected void handleEvent(AbstractTaskEvent evt) {
		this.eventHandler.deliverEvent(evt);
	}
	
	@Override
	public void setIterative(int numEventsUntilEndOfSuperstep) {
		this.numEventsUntilEndOfSuperstep = numEventsUntilEndOfSuperstep;
	}

	@Override
	public void startNextSuperstep() {
		if (this.numEventsUntilEndOfSuperstep == -1) {
			throw new IllegalStateException("Called 'startNextSuperstep()' in a non-iterative reader.");
		}
		else if (endOfSuperstepEventsCount < numEventsUntilEndOfSuperstep) {
			throw new IllegalStateException("Premature 'startNextSuperstep()'. Not yet reached the end-of-superstep.");
		}
		this.endOfSuperstepEventsCount = 0;
	}
	
	@Override
	public boolean hasReachedEndOfSuperstep() {
		return endOfSuperstepEventsCount== numEventsUntilEndOfSuperstep;
	}
	
	protected boolean incrementEndOfSuperstepEventAndCheck() {
		if (numEventsUntilEndOfSuperstep == -1)
			throw new IllegalStateException("Received EndOfSuperstep event in a non-iterative reader.");
		
		endOfSuperstepEventsCount++;
		
		if (endOfSuperstepEventsCount > numEventsUntilEndOfSuperstep)
			throw new IllegalStateException("Received EndOfSuperstep events beyond the number to indicate the end of the superstep");
		
		return endOfSuperstepEventsCount== numEventsUntilEndOfSuperstep;
	}
}
