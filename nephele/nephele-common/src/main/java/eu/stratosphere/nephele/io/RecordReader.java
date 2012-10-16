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
 *        The type of the record that can be read from this record reader.
 */

public class RecordReader<T extends Record> extends AbstractRecordReader<T> implements Reader<T> {

	/**
	 * Stores the last read record.
	 */
	private T lastRead;

	/**
	 * Temporarily stores an exception which may have occurred while reading data from the input gate.
	 */
	private IOException ioException;

	/**
	 * Temporarily stores an exception which may have occurred while reading data from the input gate.
	 */
	private InterruptedException interruptedException;

	/**
	 * Stores if more no more records will be received from the assigned input gate.
	 */
	private boolean noMoreRecordsWillFollow;

	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 */
	public RecordReader(final AbstractTask taskBase, final Class<T> inputClass) {

		super(taskBase, new DefaultRecordFactory<T>(inputClass));
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param inputClass
	 *        the class of records that can be read from the record reader
	 */
	public RecordReader(final AbstractOutputTask outputBase, final Class<T> inputClass) {

		super(outputBase, new DefaultRecordFactory<T>(inputClass));
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param recordFactory
	 *        a factory to instantiate new record records
	 */
	public RecordReader(final AbstractTask taskBase, final RecordFactory<T> recordFactory) {

		super(taskBase, recordFactory);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param recordFactory
	 *        a factory to instantiate new record records
	 */
	public RecordReader(final AbstractOutputTask outputBase, final RecordFactory<T> recordFactory) {

		super(outputBase, recordFactory);
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
	public boolean hasNext()
	{
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
	public T next() throws IOException, InterruptedException
	{
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
}
