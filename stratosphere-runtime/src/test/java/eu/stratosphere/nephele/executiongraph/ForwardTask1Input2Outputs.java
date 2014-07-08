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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.runtime.io.api.RecordReader;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class ForwardTask1Input2Outputs extends AbstractInvokable {

	private RecordReader<StringRecord> input = null;

	private RecordWriter<StringRecord> output1 = null;

	private RecordWriter<StringRecord> output2 = null;

	@Override
	public void invoke() throws Exception {

		this.output1.initializeSerializers();
		this.output2.initializeSerializers();

		while (this.input.hasNext()) {

			StringRecord s = input.next();
			this.output1.emit(s);
			this.output2.emit(s);
		}

		this.output1.flush();
		this.output2.flush();
	}

	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<StringRecord>(this, StringRecord.class);
		this.output1 = new RecordWriter<StringRecord>(this);
		this.output2 = new RecordWriter<StringRecord>(this);
	}
}
