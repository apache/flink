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

package eu.stratosphere.nephele.io;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

public interface Gate<T extends Record> {

	/**
	 * Returns the index that has been assigned to the gate upon initialization.
	 * 
	 * @return the index that has been assigned to the gate upon initialization.
	 */
	int getIndex();

	/**
	 * Subscribes the listener object to receive events of the given type.
	 * 
	 * @param eventListener
	 *        the listener object to register
	 * @param eventType
	 *        the type of event to register the listener for
	 */
	void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType);

	/**
	 * Removes the subscription for events of the given type for the listener object.
	 * 
	 * @param eventListener
	 *        the listener object to cancel the subscription for
	 * @param eventType
	 *        the type of the event to cancel the subscription for
	 */
	void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType);

	/**
	 * Publishes an event.
	 * 
	 * @param event
	 *        the event to be published
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the event
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the event to be published
	 */
	void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException;

	/**
	 * Passes a received event on to the event notification manager so it cam ne dispatched.
	 * 
	 * @param event
	 *        the event to pass on to the notification manager
	 */
	void deliverEvent(AbstractTaskEvent event);

	/**
	 * Returns the ID of the job this gate belongs to.
	 * 
	 * @return the ID of the job this gate belongs to
	 */
	JobID getJobID();

	/**
	 * Returns the type of the input/output channels which are connected to this gate.
	 * 
	 * @return the type of input/output channels which are connected to this gate
	 */
	ChannelType getChannelType();

	/**
	 * Returns the compression level that is applied by the input/output channels attached to this gate.
	 * 
	 * @return the compression level that is applied by the input/output channels attached to this gate
	 */
	CompressionLevel getCompressionLevel();

	/**
	 * Returns the ID of the gate.
	 * 
	 * @return the ID of the gate
	 */
	GateID getGateID();

	/**
	 * Releases the allocated resources (particularly buffer) of all channels attached to this gate. This method
	 * should only be called after the respected task has stopped running.
	 */
	void releaseAllChannelResources();

	/**
	 * Checks if the gate is closed. The gate is closed if all this associated channels are closed.
	 * 
	 * @return <code>true</code> if the gate is closed, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if any error occurred while closing the gate
	 * @throws InterruptedException
	 *         thrown if the gate is interrupted while waiting for this operation to complete
	 */
	boolean isClosed() throws IOException, InterruptedException;

	/**
	 * Checks if the considered gate is an input gate.
	 * 
	 * @return <code>true</code> if the considered gate is an input gate, <code>false</code> if it is an output gate
	 */
	boolean isInputGate();
}
