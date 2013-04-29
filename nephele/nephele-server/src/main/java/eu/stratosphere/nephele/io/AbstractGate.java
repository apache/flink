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

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.event.task.EventNotificationManager;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

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
public abstract class AbstractGate<T extends Record> implements Gate<T> {

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
	 * The compression level of the input/output channels connected to this gate.
	 */
	private CompressionLevel compressionLevel = CompressionLevel.NO_COMPRESSION;

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
	 * {@inheritDoc}
	 */
	@Override
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
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return "Gate " + this.index;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void unsubscribeFromEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		this.eventNotificationManager.unsubscribeFromEvent(eventListener, eventType);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void deliverEvent(final AbstractTaskEvent event) {

		this.eventNotificationManager.deliverEvent((AbstractTaskEvent) event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void setChannelType(final ChannelType channelType) {

		this.channelType = channelType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final ChannelType getChannelType() {

		return this.channelType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void setCompressionLevel(final CompressionLevel compressionLevel) {

		this.compressionLevel = compressionLevel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final CompressionLevel getCompressionLevel() {

		return this.compressionLevel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getGateID() {

		return this.gateID;
	}
}
