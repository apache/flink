/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.example.union;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.UnionRecordReader;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.StringRecord;

public class UnionTask extends AbstractTask {

	private UnionRecordReader<StringRecord> input;

	private RecordWriter<StringRecord> output;

	@Override
	public void registerInputOutput() {

		@SuppressWarnings("unchecked")
		final RecordReader<StringRecord>[] recordReaders = (RecordReader<StringRecord>[]) new RecordReader<?>[2];
		recordReaders[0] = new RecordReader<StringRecord>(this, StringRecord.class);
		recordReaders[1] = new RecordReader<StringRecord>(this, StringRecord.class);

		this.input = new UnionRecordReader<StringRecord>(recordReaders);
		this.output = new RecordWriter<StringRecord>(this);
	}

	@Override
	public void invoke() throws Exception {

		while (this.input.hasNext()) {
			this.output.emit(this.input.next());
		}

	}

}
