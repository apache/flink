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

package eu.stratosphere.pact.runtime.iterative.io;

import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.runtime.io.api.MutableRecordReader;
import eu.stratosphere.types.Record;

/**
 * Output task for the iteration tail
 */
public class FakeOutputTask extends AbstractInvokable {

	private MutableRecordReader<Record> reader;

	private final Record record = new Record();

	@Override
	public void registerInputOutput() {
		reader = new MutableRecordReader<Record>(this);
	}

	@Override
	public void invoke() throws Exception {
		// ensure that input is consumed, although this task should never see any records
		while (reader.next(record)) {
			throw new IllegalStateException("This task should not receive any data");
		}
	}
}
