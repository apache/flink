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
 * This is an abstract base class for a record reader, either dealing with mutable or immutable records.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record that can be read from this record reader
 */
public abstract class AbstractRecordReader<T extends Record> {

	/**
	 * The input gate associated with the record reader.
	 */
	private InputGate<T> inputGate = null;

	/**
	 * The environment the associated task runs in.
	 */
	private final Environment environment;

	protected AbstractRecordReader(final AbstractInvokable invokable, final RecordDeserializer<T> deserializer,
			final int inputGateID, final DistributionPattern distributionPattern) {

		this.environment = invokable.getEnvironment();
		connectInputGate(deserializer, inputGateID, distributionPattern);
	}

	/**
	 * Connects a record reader to an input gate.
	 * 
	 * @param inputClass
	 *        the class of the record that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	// TODO: See if type safety can be improved here
	@SuppressWarnings("unchecked")
	private void connectInputGate(final RecordDeserializer<T> deserializer, final int inputGateID,
			final DistributionPattern distributionPattern) {

		GateID gateID = this.environment.getNextUnboundInputGateID();
		if (gateID == null) {
			gateID = new GateID();
		}

		this.inputGate = (InputGate<T>) this.environment.createInputGate(gateID, deserializer, distributionPattern);
		this.environment.registerInputGate(this.inputGate);
	}

	/**
	 * Returns the number of input channels wired to this reader's input gate.
	 * 
	 * @return the number of input channels wired to this reader's input gate
	 */
	public final int getNumberOfInputChannels() {
		return this.inputGate.getNumberOfInputChannels();
	}

	/**
	 * Checks if the input channel with the given index is closed.
	 * 
	 * @param index
	 *        the index of the input channel
	 * @return <code>true</code> if the respective input channel is already closed, otherwise <code>false</code>
	 * @throws IOException
	 *         thrown if an error occurred while closing the input channel
	 * @throws InterruptedException
	 *         thrown if the channel is interrupted while processing this call
	 */
	public final boolean isInputChannelClosed(final int index) throws IOException, InterruptedException {

		if (index < this.inputGate.getNumberOfInputChannels()) {
			return this.inputGate.getInputChannel(index).isClosed();
		}

		return false;
	}

	/**
	 * Subscribes the listener object to receive events of the given type.
	 * 
	 * @param eventListener
	 *        the listener object to register
	 * @param eventType
	 *        the type of event to register the listener for
	 */
	public final void subscribeToEvent(final EventListener eventListener,
			final Class<? extends AbstractTaskEvent> eventType) {

		// Delegate call to input gate
		this.inputGate.subscribeToEvent(eventListener, eventType);
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

		// Delegate call to input gate
		this.inputGate.unsubscribeFromEvent(eventListener, eventType);
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
	public final void publishEvent(final AbstractTaskEvent event) throws IOException, InterruptedException {

		// Delegate call to input gate
		this.inputGate.publishEvent(event);
	}

	/**
	 * Protected method for the subclasses to access the input gate.
	 * 
	 * @return the input gate
	 */
	protected InputGate<T> getInputGate() {

		return this.inputGate;
	}
}
