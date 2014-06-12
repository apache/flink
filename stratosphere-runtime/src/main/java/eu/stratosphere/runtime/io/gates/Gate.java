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

package eu.stratosphere.runtime.io.gates;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.event.task.EventNotificationManager;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;

import java.io.IOException;

/**
 * In Nephele a gate represents the connection between a user program and the processing framework. A gate
 * must be connected to exactly one record reader/writer and to at least one channel. The <code>Gate</code> class itself
 * is abstract. A gate automatically created for every record reader/writer in the user program. A gate can only be used
 * to transport one specific type of records.
 * <p>
 * This class in general is not thread-safe.
 * 
 * @param <T>
 *        the record type to be transported from this gate
 *
 *  TODO refactor with changes to input side
 */
public abstract class Gate<T extends IOReadableWritable> {

	/**
	 * The ID of the job this gate belongs to.
	 */
	private final JobID jobID;

	/**
	 * The ID of this gate.
	 */
	private final GateID gateID;

	/**
	 * The index of the gate in the list of available input/output gates.
	 */
	private final int index;

	/**
	 * The event notification manager used to dispatch events.
	 */
	private final EventNotificationManager eventNotificationManager = new EventNotificationManager();

	/**
	 * The type of input/output channels connected to this gate.
	 */
	private ChannelType channelType = ChannelType.NETWORK;

	/**
	 * Constructs a new abstract gate
	 * 
	 * @param jobID
	 *        the ID of the job this gate belongs to
	 * @param gateID
	 *        the ID of this gate
	 * @param index
	 *        the index of the gate in the list of available input/output gates.
	 */
	protected Gate(final JobID jobID, final GateID gateID, final int index) {
		this.jobID = jobID;
		this.gateID = gateID;
		this.index = index;
	}

	public final int getIndex() {
		return this.index;
	}

	/**
	 * Returns the event notification manager used to dispatch events.
	 * 
	 * @return the event notification manager used to dispatch events
	 */
	protected final EventNotificationManager getEventNotificationManager() {
		return this.eventNotificationManager;
	}

	public String toString() {

		return "Gate " + this.index;
	}

	public final void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.subscribeToEvent(eventListener, eventType);
	}

	public final void unsubscribeFromEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.unsubscribeFromEvent(eventListener, eventType);
	}

	public final void deliverEvent(final AbstractTaskEvent event) {

		this.eventNotificationManager.deliverEvent((AbstractTaskEvent) event);
	}

	public final void setChannelType(final ChannelType channelType) {

		this.channelType = channelType;
	}

	public final ChannelType getChannelType() {

		return this.channelType;
	}

	public JobID getJobID() {

		return this.jobID;
	}

	public GateID getGateID() {

		return this.gateID;
	}

	// FROM GATE INTERFACE

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
	abstract public void publishEvent(AbstractEvent event) throws IOException, InterruptedException;

	/**
	 * Releases the allocated resources (particularly buffer) of all channels attached to this gate. This method
	 * should only be called after the respected task has stopped running.
	 */
	abstract public void releaseAllChannelResources();

	/**
	 * Checks if the gate is closed. The gate is closed if all this associated channels are closed.
	 *
	 * @return <code>true</code> if the gate is closed, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if any error occurred while closing the gate
	 * @throws InterruptedException
	 *         thrown if the gate is interrupted while waiting for this operation to complete
	 */
	abstract public boolean isClosed() throws IOException, InterruptedException;

	/**
	 * Checks if the considered gate is an input gate.
	 *
	 * @return <code>true</code> if the considered gate is an input gate, <code>false</code> if it is an output gate
	 */
	abstract public boolean isInputGate();

}
