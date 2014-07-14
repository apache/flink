/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.streaming.api.streamcomponent;

import java.util.ArrayList;

import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class MockRecordWriter extends RecordWriter<StreamRecord> {

	public ArrayList<StreamRecord> emittedRecords;

	public MockRecordWriter(DataSourceTask<?> inputBase, Class<StreamRecord> outputClass) {
		super(inputBase);
	}

	public boolean initList() {
		emittedRecords = new ArrayList<StreamRecord>();
		return true;
	}
	
	@Override
	public void emit(StreamRecord record) {
		emittedRecords.add(record.copy());
	}
}