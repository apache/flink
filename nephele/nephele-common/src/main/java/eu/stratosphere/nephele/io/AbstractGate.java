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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.event.task.EventNotificationManager;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * In Nephele a gate represents the connection between a user program and the processing framework. A gate
 * must be connected to exactly one record reader/writer and to at least one channel. The <code>Gate</code> class itself
 * is abstract. A gate automatically created for every record reader/writer in the user program. A gate can only be used
 * to transport one specific type of records.
 * <p>
 * This class in general is not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the record type to be transported from this gate
 */
public abstract class AbstractGate<T extends Record> implements IOReadableWritable {

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
	protected AbstractGate(final JobID jobID, final GateID gateID, final int index) {
		this.jobID = jobID;
		this.gateID = gateID;
		this.index = index;
	}

	/**
	 * Returns the index that has been assigned to the gate upon initialization.
	 * 
	 * @return the index that has been assigned to the gate upon initialization.
	 */
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

	/**
	 * Checks if the gate is closed. The gate is closed if alls this associated channels are closed.
	 * 
	 * @return <code>true</code> if the gate is closed, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if any error occurred while closing the gate
	 * @throws InterruptedException
	 *         thrown if the gate is interrupted while waiting for this operation to complete
	 */
	public abstract boolean isClosed() throws IOException, InterruptedException;

	/**
	 * Checks if the considered gate is an input gate.
	 * 
	 * @return <code>true</code> if the considered gate is an input gate, <code>false</code> if it is an output gate
	 */
	public abstract boolean isInputGate();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return "Gate " + this.index;
	}

	/**
	 * Subscribes the listener object to receive events of the given type.
	 * 
	 * @param eventListener
	 *        the listener object to register
	 * @param eventType
	 *        the type of event to register the listener for
	 */
	public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * Removes the subscription for events of the given type for the listener object.
	 * 
	 * @param eventListener
	 *        the listener object to cancel the subscription for
	 * @param eventType
	 *        the type of the event to cancel the subscription for
	 */
	public final void unsubscribeFromEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * Passes a received event on to the event notification manager so it cam ne dispatched.
	 * 
	 * @param event
	 *        the event to pass on to the notification manager
	 */
	public final void deliverEvent(final AbstractTaskEvent event) {

		this.eventNotificationManager.deliverEvent((AbstractTaskEvent) event);
	}

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
	public abstract void publishEvent(final AbstractTaskEvent event) throws IOException, InterruptedException;

	/**
	 * Sets the type of the input/output channels which are connected to this gate.
	 * 
	 * @param channelType
	 *        the type of input/output channels which are connected to this gate
	 */
	public final void setChannelType(final ChannelType channelType) {

		this.channelType = channelType;
	}

	/**
	 * Returns the type of the input/output channels which are connected to this gate.
	 * 
	 * @return the type of input/output channels which are connected to this gate
	 */
	public final ChannelType getChannelType() {

		return this.channelType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.channelType = EnumUtils.readEnum(in, ChannelType.class);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		EnumUtils.writeEnum(out, this.channelType);
	}

	/**
	 * Returns the ID of the job this gate belongs to.
	 * 
	 * @return the ID of the job this gate belongs to
	 */
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * Returns the ID of the gate.
	 * 
	 * @return the ID of the gate
	 */
	public GateID getGateID() {

		return this.gateID;
	}

	/**
	 * Releases the allocated resources (particularly buffer) of all channels attached to this gate. This method
	 * should only be called after the respected task has stopped running.
	 */
	public abstract void releaseAllChannelResources();
}
