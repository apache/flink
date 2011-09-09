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
import java.util.List;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;

/**
 * A record reader connects the application to an output gate. It allows the application
 * of emit (send out) to the output gate. The output gate will then take care of distributing
 * the emitted records among the output channels.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record that can be emitted with this record writer
 */

public class RecordWriter<T extends Record> implements Writer<T> {

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
	 * @param taskBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 * @param partitionier
	 *        the channel selector to be used to determine the output channel to be used for a record
	 */
	public RecordWriter(AbstractTask taskBase, Class<T> outputClass, ChannelSelector<T> selector) {

		this.environment = taskBase.getEnvironment();
		connectOutputGate(outputClass, selector);

	}

	/**
	 * Constructs a new record writer and registers a new output gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 */
	public RecordWriter(AbstractTask taskBase, Class<T> outputClass) {

		this.environment = taskBase.getEnvironment();
		connectOutputGate(outputClass, null);

	}

	public RecordWriter(AbstractInputTask inputBase, Class<T> outputClass) {

		this.environment = inputBase.getEnvironment();
		connectOutputGate(outputClass, null);
	}

	public RecordWriter(AbstractInputTask inputBase, Class<T> outputClass, ChannelSelector<T> selector) {
		this.environment = inputBase.getEnvironment();
		connectOutputGate(outputClass, selector);
	}

	/**
	 * Connects a record writer to an output gate.
	 * 
	 * @param outputClass
	 *        the class of the record that can be emitted with this record writer
	 */
	// TODO: See if type safety can be improved here
	@SuppressWarnings("unchecked")
	private void connectOutputGate(Class<T> outputClass, ChannelSelector<T> selector) {

		// See if there are any unbound input gates left we can connect to
		if (this.environment.hasUnboundOutputGates()) {
			final OutputGate<T> eog = (OutputGate<T>) this.environment.getUnboundOutputGate(0);
			if (!outputClass.equals(eog.getType())) {
				throw new RuntimeException("Unbound input gate found, but types do not match!");
			}

			this.outputGate = eog;
		} else {
			this.outputGate = new OutputGate<T>(outputClass, this.environment.getNumberOfOutputGates(), selector);
			this.environment.registerOutputGate(this.outputGate);
		}
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
	public void emit(T record) throws IOException, InterruptedException {

		// Simply pass record through to the corresponding output gate
		this.outputGate.writeRecord(record);
	}

	/**
	 * Returns the list of OutputChannels connected to this RecordWriter.
	 * 
	 * @return the list of OutputChannels connected to this RecordWriter
	 */
	public List<AbstractOutputChannel<T>> getOutputChannels() {
		return this.outputGate.getOutputChannels();
	}

	/**
	 * Registers a new listener object with the assigned output gate.
	 * 
	 * @param inputGateListener
	 *        the listener object to register
	 */
	public void registerOutputGateListener(OutputGateListener outputGateListener) {

		this.outputGate.registerOutputGateListener(outputGateListener);
	}

	// TODO (en)
	public OutputGate<T> getOutputGate() {
		return outputGate;
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
