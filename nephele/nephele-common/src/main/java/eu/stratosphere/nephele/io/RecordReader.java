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
 * @param <T> The type of the record that can be read from this record reader.
 */
public class RecordReader<T extends Record> extends AbstractSingleGateRecordReader<T> implements Reader<T> {
	
	private final Class<T> recordType;
	
	/**
	 * Stores the last read record.
	 */
	private T lookahead;

	/**
	 * Stores if more no more records will be received from the assigned input gate.
	 */
	private boolean noMoreRecordsWillFollow;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        The application that instantiated the record reader.
	 * @param recordType
	 *        The class of records that can be read from the record reader.
	 */
	public RecordReader(AbstractTask taskBase, Class<T> recordType) {
		super(taskBase, MutableRecordDeserializerFactory.<T>get(), 0);
		this.recordType = recordType;
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        The application that instantiated the record reader.
	 * @param recordType
	 *        The class of records that can be read from the record reader.
	 */
	public RecordReader(AbstractOutputTask outputBase, Class<T> recordType) {
		super(outputBase, MutableRecordDeserializerFactory.<T>get(), 0);
		this.recordType = recordType;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Checks if at least one more record can be read from the associated input gate. This method may block
	 * until the associated input gate is able to read the record from one of its input channels.
	 * 
	 * @return <code>true</code>it at least one more record can be read from the associated input gate, otherwise
	 *         <code>false</code>
	 */
	@Override
	public boolean hasNext() throws IOException, InterruptedException{
		if (this.lookahead != null) {
			return true;
		} else {
			if (this.noMoreRecordsWillFollow) {
				return false;
			}
			
			T record = instantiateRecordType();
			
			while (true) {
				InputChannelResult result = this.inputGate.readRecord(record);
				switch (result) {
					case INTERMEDIATE_RECORD_FROM_BUFFER:
					case LAST_RECORD_FROM_BUFFER:
						this.lookahead = record;
						return true;
						
					case END_OF_SUPERSTEP:
						if (incrementEndOfSuperstepEventAndCheck())
							return false;
						else 
							break; // fall through and wait for next record/event
						
					case TASK_EVENT:
						handleEvent(this.inputGate.getCurrentEvent());
						break;
						
					case END_OF_STREAM:
						this.noMoreRecordsWillFollow = true;
						return false;
				
					default:
						; // fall through the loop
				}
			}
		}
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
		if (hasNext()) {
			T tmp = this.lookahead;
			this.lookahead = null;
			return tmp;
		} else {
			return null;
		}
	}
	
	@Override
	public boolean isInputClosed() {
		return this.noMoreRecordsWillFollow;
	}
	
	private T instantiateRecordType() {
		try {
			return this.recordType.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException("Cannot instantiate class '" + this.recordType.getName() + "'.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Cannot instantiate class '" + this.recordType.getName() + "'.", e);
		}
	}
}
