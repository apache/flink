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

package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.UnionRecordReader;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * A simple implementation of a task using a {@link UnionRecordReader}.
 */
public class UnionTask extends AbstractTask {

	/**
	 * The union record reader to be used during the tests.
	 */
	private UnionRecordReader<StringRecord> unionReader;

	private RecordWriter<StringRecord> writer;
	
	
	@Override
	public void registerInputOutput() {

		@SuppressWarnings("unchecked")
		MutableRecordReader<StringRecord>[] recordReaders = (MutableRecordReader<StringRecord>[]) new MutableRecordReader<?>[2];
		recordReaders[0] = new MutableRecordReader<StringRecord>(this);
		recordReaders[1] = new MutableRecordReader<StringRecord>(this);
		this.unionReader = new UnionRecordReader<StringRecord>(recordReaders, StringRecord.class);
		
		this.writer = new RecordWriter<StringRecord>(this, StringRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		while (this.unionReader.hasNext()) {
			this.writer.emit(this.unionReader.next());
		}
	}
}
