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
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;

/**
 * A record writer connects an input gate to an application. It allows the application
 * query for incoming records and read them from input gate.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record that can be read from this record reader
 */

// FIXME added Writer<T> to make this at least mock-able ... still requires refactoring (en)
public class RecordReader<T extends Record> implements Reader<T> {

	/**
	 * The input gate associated with the record reader.
	 */
	private InputGate<T> inputGate = null;

	/**
	 * The environment the associated task runs in.
	 */
	private Environment environment = null;

	/**
	 * Temporarily stores an exception which may have occurred while reading data from the input gate.
	 */
	private IOException ioException = null;

	private InterruptedException interruptedException = null;

	/**
	 * Stores the last read record.
	 */
	private T lastRead = null;

	/**
	 * Stores if more no more records will be received from the assigned input gate.
	 */
	private boolean noMoreRecordsWillFollow = false;

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractTask taskBase, Class<T> inputClass, DistributionPattern distributionPattern) {

		this.environment = taskBase.getEnvironment();
		connectInputGate(new DefaultRecordDeserializer<T>(inputClass), 0, distributionPattern);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractOutputTask outputBase, Class<T> inputClass, DistributionPattern distributionPattern) {

		this.environment = outputBase.getEnvironment();
		connectInputGate(new DefaultRecordDeserializer<T>(inputClass), 0, distributionPattern);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractTask taskBase, RecordDeserializer<T> deserializer) {
		this.environment = taskBase.getEnvironment();
		connectInputGate(deserializer, 0, null);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractTask taskBase, RecordDeserializer<T> deserializer, int inputGateID) {
		this.environment = taskBase.getEnvironment();
		connectInputGate(deserializer, inputGateID, null);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractTask taskBase, RecordDeserializer<T> deserializer,
			DistributionPattern distributionPattern) {
		this.environment = taskBase.getEnvironment();
		connectInputGate(deserializer, 0, distributionPattern);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractTask outputBase, RecordDeserializer<T> deserializer, int inputGateID,
			DistributionPattern distributionPattern) {

		this.environment = outputBase.getEnvironment();
		connectInputGate(deserializer, inputGateID, distributionPattern);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractOutputTask outputBase, RecordDeserializer<T> deserializer,
			DistributionPattern distributionPattern) {

		this.environment = outputBase.getEnvironment();
		connectInputGate(deserializer, 0, distributionPattern);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public RecordReader(AbstractOutputTask outputBase, RecordDeserializer<T> deserializer, int inputGateID,
			DistributionPattern distributionPattern) {

		this.environment = outputBase.getEnvironment();
		connectInputGate(deserializer, inputGateID, distributionPattern);
	}

	/**
	 * Returns the number of input channels wired to this reader's input gate.
	 * 
	 * @return the number of input channels wired to this reader's input gate
	 */
	public int getNumberOfInputChannels() {
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
	 */
	public boolean isInputChannelClosed(int index) throws IOException {

		if (index < this.inputGate.getNumberOfInputChannels()) {
			return this.inputGate.getInputChannel(index).isClosed();
		}

		return false;
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
	private void connectInputGate(RecordDeserializer<T> deserializer, int inputGateID,
			DistributionPattern distributionPattern) {

		// See if there are any unbound input gates left we can connect to
		if (this.environment.hasUnboundInputGates()) {
			final InputGate<T> ig = (InputGate<T>) this.environment.getUnboundInputGate(inputGateID);
			if (!deserializer.getRecordType().equals(ig.getType())) {
				throw new RuntimeException("Unbound input gate found, but types do not match!");
			}

			this.inputGate = ig;
		} else {
			this.inputGate = new InputGate<T>(deserializer, this.environment.getNumberOfInputGates(),
				distributionPattern);
			this.environment.registerInputGate(this.inputGate);
		}
	}

	/**
	 * Checks if at least one more record can be read from the associated input gate. This method may block
	 * until the associated input gate is able to read the record from one of its input channels.
	 * 
	 * @return <code>true</code>it at least one more record can be read from the associated input gate, otherwise
	 *         <code>false</code>
	 */
	public boolean hasNext() {

		if (this.noMoreRecordsWillFollow) {
			return false;
		}

		if (this.lastRead == null) {
			try {
				this.lastRead = inputGate.readRecord();
				if (this.lastRead == null) {
					return false;
				}
			} catch (IOException e) {
				this.ioException = e;
				return true;
			} catch (InterruptedException e) {
				this.interruptedException = e;
				return true;
			}

		}

		return true;
	}

	/**
	 * Reads the current record from the associated input gate.
	 * 
	 * @return the current record from the associated input gate.
	 * @throws IOException
	 *         thrown if any error occurs while reading the record from the input gate
	 */
	public T next() throws IOException, InterruptedException {

		if (this.ioException != null) {
			throw this.ioException;
		}
		if (this.interruptedException != null) {
			throw this.interruptedException;
		}

		final T retVal = this.lastRead;
		this.lastRead = this.inputGate.readRecord();
		if (this.lastRead == null) {
			this.noMoreRecordsWillFollow = true;
		}

		return retVal;
	}

	/**
	 * Returns the list of InputChannels that feed this RecordReader.
	 * 
	 * @return the list of InputChannels that feed this RecordReader
	 */
	public List<AbstractInputChannel<T>> getInputChannels() {
		return this.inputGate.getInputChannels();
	}

	/**
	 * Registers a new listener object with the assigned input gate.
	 * 
	 * @param inputGateListener
	 *        the listener object to register
	 */
	public void registerInputGateListener(InputGateListener inputGateListener) {

		this.inputGate.registerInputGateListener(inputGateListener);
	}

	public void subscribeToEvent(EventListener eventNotifiable, Class<? extends AbstractTaskEvent> eventType) {

		// Delegate call to input gate
		this.inputGate.subscribeToEvent(eventNotifiable, eventType);
	}

	public void unsubscribeFromEvent(EventListener eventNotifiable, Class<? extends AbstractTaskEvent> eventType) {

		// Delegate call to input gate
		this.inputGate.unsubscribeFromEvent(eventNotifiable, eventType);
	}

	public void publishEvent(AbstractTaskEvent event) throws IOException {

		// Delegate call to input gate
		this.inputGate.publishEvent(event);
	}
}
