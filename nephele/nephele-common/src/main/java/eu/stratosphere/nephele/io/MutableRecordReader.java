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

public class MutableRecordReader<T extends Record> extends AbstractRecordReader<T> implements MutableReader<T> {

	/**
	 * Constructs a new mutable record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public MutableRecordReader(final AbstractTask taskBase) {

		super(taskBase, new MutableRecordDeserializer<T>(), 0);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 */
	public MutableRecordReader(final AbstractOutputTask outputBase) {

		super(outputBase, new MutableRecordDeserializer<T>(), 0);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record reader
	 * @param inputGateID
	 */
	public MutableRecordReader(final AbstractTask taskBase, final int inputGateID) {

		super(taskBase, new MutableRecordDeserializer<T>(), inputGateID);
	}

	/**
	 * Constructs a new record reader and registers a new input gate with the application's environment.
	 * 
	 * @param outputBase
	 *        the application that instantiated the record reader
	 * @param inputGateID
	 * @param distributionPattern
	 *        the {@link DistributionPattern} that should be used for rewiring
	 */
	public MutableRecordReader(final AbstractOutputTask outputBase, int inputGateID) {

		super(outputBase, new MutableRecordDeserializer<T>(), inputGateID);
	}

	@Override
	public boolean next(final T target) throws IOException, InterruptedException {

		final T record = getInputGate().readRecord(target);
		if (record == null) {
			return false;
		}

		return true;
	}
}
