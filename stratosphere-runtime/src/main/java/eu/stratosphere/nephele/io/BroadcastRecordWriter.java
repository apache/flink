/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * A record writer connects the application to an output gate. It allows the application
 * of emit (send out) to the output gate. The broadcast record writer will make sure that each emitted record will be
 * transfered via all connected output channels.
 * 
 * @author warneke
 * @param <T>
 *        the type of the record that can be emitted with this record writer
 */
public class BroadcastRecordWriter<T extends IOReadableWritable> extends AbstractRecordWriter<T> {

	/**
	 * Constructs a new broadcast record writer and registers a new output gate with the application's environment.
	 * 
	 * @param taskBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 */
	public BroadcastRecordWriter(AbstractTask taskBase, Class<T> outputClass) {
		super(taskBase, outputClass, null, true);
	}

	/**
	 * Constructs a new broadcast record writer and registers a new output gate with the application's environment.
	 * 
	 * @param inputBase
	 *        the application that instantiated the record writer
	 * @param outputClass
	 *        the class of records that can be emitted with this record writer
	 */
	public BroadcastRecordWriter(AbstractInputTask<?> inputBase, Class<T> outputClass) {
		super(inputBase, outputClass, null, true);
	}
}
