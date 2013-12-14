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

package eu.stratosphere.nephele.example.events;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.event.task.StringTaskEvent;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * @author casp
 */
public class EventReceiver extends AbstractTask {

	// this is just a dummy input gate...
	private RecordReader<StringRecord> input = null;

	private RecordWriter<StringRecord> output = null;

	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<StringRecord>(this, StringRecord.class);
		this.output = new RecordWriter<StringRecord>(this, StringRecord.class);
		this.input.subscribeToEvent(new MyEventListener(), StringTaskEvent.class);
	}

	@Override
	public void invoke() throws Exception {

		while (this.input.hasNext()) {

			StringRecord s = input.next();
			this.output.emit(s);
		}

	}

}
