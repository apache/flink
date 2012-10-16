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
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;

/**
 * Abstract base class for a regular record writer and broadcast record writer.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record that can be emitted with this record writer
 */
public abstract class AbstractRecordWriter<T extends Record> implements Writer<T> {

	/**
	 * The output gate assigned to this record writer.
	 */
	private OutputGate<T> outputGate = null;

	/**
	 * The environment associated to this record writer.
	 */
	private Environment environment = null;

	/**
	 * Constructs a new record writer and registers a new output gate with the application's environment.
	 * 
	 * @param invokable
	 *        the application that instantiated the record writer
	 * @param selector
	 *        the channel selector to be used to determine the output channel to be used for a record
	 * @param isBroadcast
	 *        <code>true</code> if this record writer shall broadcast the records to all connected channels,
	 *        <code>false/<code> otherwise
	 */
	public AbstractRecordWriter(final AbstractInvokable invokable, final ChannelSelector<T> selector, final boolean isBroadcast) {

		this.environment = invokable.getEnvironment();
		connectOutputGate(selector, isBroadcast);
	}

	/**
	 * Connects a record writer to an output gate.
	 * 
	 * @param selector
	 *        the channel selector to be used to determine the output channel to be used for a record
	 * @param isBroadcast
	 *        <code>true</code> if this record writer shall broadcast the records to all connected channels,
	 *        <code>false/<code> otherwise
	 */
	private void connectOutputGate(final ChannelSelector<T> selector, final boolean isBroadcast) {
		
		GateID gateID = this.environment.getNextUnboundOutputGateID();
		if (gateID == null) {
			gateID = new GateID();
		}

		this.outputGate = this.environment.createOutputGate(gateID, selector, isBroadcast);
		this.environment.registerOutputGate(this.outputGate);
	}

	/**
	 * This method emits a record to the corresponding output gate. The method may block
	 * until the record was transfered via any of the connected channels.
	 * 
	 * @param record
	 *        The record to be emitted.
	 * @throws IOException
	 *         Thrown on an error that may happen during the transfer of the given record or a previous record.
	 */
	public void emit(final T record) throws IOException, InterruptedException {

		// Simply pass record through to the corresponding output gate
		this.outputGate.writeRecord(record);
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

		// Delegate call to output gate
		this.outputGate.subscribeToEvent(eventListener, eventType);
	}

	/**
	 * Removes the subscription for events of the given type for the listener object.
	 * 
	 * @param eventListener
	 *        the listener object to cancel the subscription for
	 * @param eventType
	 *        the type of the event to cancel the subscription for
	 */
	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

		// Delegate call to output gate
		this.outputGate.unsubscribeFromEvent(eventListener, eventType);
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
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Delegate call to output gate
		this.outputGate.publishEvent(event);
	}

	public void flush() throws IOException, InterruptedException {
		// Delegate call to output gate
		this.outputGate.flush();
	}
}
