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

import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;

/**
 * A record writer connects the application to an output gate. It allows the application
 * of emit (send out) to the output gate. The output gate will then take care of distributing
 * the emitted records among the output channels.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends Record> extends AbstractRecordWriter<T> {

	/**
	 * Constructs a new record writer and registers a new output gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 * @param selector
	 *        the channel selector to be used to determine the output channel to be used for a record
	 */
	public RecordWriter(AbstractTask taskBase, Class<T> outputClass, ChannelSelector<T> selector) {
		super(taskBase, outputClass, selector, false);
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
		super(taskBase, outputClass, null, false);
	}

	/**
	 * This method emits a record to the corresponding output gate. The method may block
	 * until the record was transfered via any of the connected channels.
	 * 
	 * @param inputBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 */
	public RecordWriter(AbstractInputTask<?> inputBase, Class<T> outputClass) {
		super(inputBase, outputClass, null, false);
	}

	/**
	 * Constructs a new record writer and registers a new output gate with the application's environment.
	 * 
	 * @param inputBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 * @param selector
	 *        the channel selector to be used to determine the output channel to be used for a record
	 */
	public RecordWriter(AbstractInputTask<?> inputBase, Class<T> outputClass, ChannelSelector<T> selector) {
		super(inputBase, outputClass, selector, false);
	}
}
