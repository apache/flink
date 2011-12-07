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

public class RecordReader<T extends Record> extends AbstractRecordReader<T> implements Reader<T> {

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

		super(taskBase, new DefaultRecordDeserializer<T>(inputClass), 0, distributionPattern);
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

		super(outputBase, new DefaultRecordDeserializer<T>(inputClass), 0, distributionPattern);
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

		super(taskBase, deserializer, 0, null);
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

		super(taskBase, deserializer, inputGateID, null);
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

		super(taskBase, deserializer, 0, distributionPattern);
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

		super(outputBase, deserializer, inputGateID, distributionPattern);
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

		super(outputBase, deserializer, 0, distributionPattern);
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

		super(outputBase, deserializer, inputGateID, distributionPattern);
	}

	/**
	 * Checks if at least one more record can be read from the associated input gate. This method may block
	 * until the associated input gate is able to read the record from one of its input channels.
	 * 
	 * @return <code>true</code>it at least one more record can be read from the associated input gate, otherwise
	 *         <code>false</code>
	 */
	@Override
	public boolean hasNext() {

		if (this.noMoreRecordsWillFollow) {
			return false;
		}

		if (this.lastRead == null) {
			try {
				this.lastRead = getInputGate().readRecord(null);
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
	@Override
	public T next() throws IOException, InterruptedException {

		if (this.ioException != null) {
			throw this.ioException;
		}
		if (this.interruptedException != null) {
			throw this.interruptedException;
		}

		final T retVal = this.lastRead;
		this.lastRead = getInputGate().readRecord(null);
		if (this.lastRead == null) {
			this.noMoreRecordsWillFollow = true;
		}

		return retVal;
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

	/**
	 * Subscribes the listener object to receive events of the given type.
	 * 
	 * @param eventListener
	 *        the listener object to register
	 * @param eventType
	 *        the type of event to register the listener for
	 */
	public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

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
	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {

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
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Delegate call to input gate
		this.inputGate.publishEvent(event);
	}

	/**
	 * Exposes the input gate which is used by this record reader. This method should have
	 * package visibility only.
	 * 
	 * @return the input gate used by this record reader
	 */
	InputGate<T> getInputGate() {

		return this.inputGate;
	}
}
