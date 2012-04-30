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

package eu.stratosphere.nephele.example.union;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;
import eu.stratosphere.nephele.types.StringRecord;

public class ConsumerTask extends AbstractFileOutputTask {

	private RecordReader<StringRecord> input;

	@Override
	public void registerInputOutput() {

		this.input = new RecordReader<StringRecord>(this, StringRecord.class);

	}

	@Override
	public void invoke() throws Exception {

		int count = 0;

		while (this.input.hasNext()) {
			this.input.next();
			++count;
		}

		System.out.println("Consumer receiver " + count + " records in total");
	}

}
