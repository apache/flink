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

package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.runtime.io.api.MutableRecordReader;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.runtime.io.api.UnionRecordReader;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.types.Record;

/**
 * A simple implementation of a task using a {@link UnionRecordReader}.
 */
public class UnionTask extends AbstractTask {

	/**
	 * The union record reader to be used during the tests.
	 */
	private UnionRecordReader<Record> unionReader;

	private RecordWriter<Record> writer;
	
	
	@Override
	public void registerInputOutput() {

		@SuppressWarnings("unchecked")
		MutableRecordReader<Record>[] recordReaders = (MutableRecordReader<Record>[]) new
				MutableRecordReader<?>[2];
		recordReaders[0] = new MutableRecordReader<Record>(this);
		recordReaders[1] = new MutableRecordReader<Record>(this);
		this.unionReader = new UnionRecordReader<Record>(recordReaders, Record.class);
		
		this.writer = new RecordWriter<Record>(this);
	}

	@Override
	public void invoke() throws Exception {
		this.writer.initializeSerializers();

		while (this.unionReader.hasNext()) {
			this.writer.emit(this.unionReader.next());
		}

		this.writer.flush();
	}
}
