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

package eu.stratosphere.streaming.test;

import eu.stratosphere.streaming.api.AtomRecord;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.test.cellinfo.WorkerEngineExact;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;

public class TestTaskInvokable extends UserTaskInvokable {

	private WorkerEngineExact engine = new WorkerEngineExact(10, 1000, 0);

	@Override
	public void invoke(StreamRecord record) throws Exception {
		IntValue value1 = (IntValue) record.getField(0, 0);
		LongValue value2 = (LongValue) record.getField(0, 1);

		// INFO
		if (record.getNumOfFields() == 2) {
			engine.put(value1.getValue(), value2.getValue());
			emit(new StreamRecord(new AtomRecord(new StringValue(value1 + " "
					+ value2))));
		}
		// QUERY
		else if (record.getNumOfFields() == 3) {
			LongValue value3 = (LongValue) record.getField(0, 2);
			emit(new StreamRecord(new AtomRecord(new StringValue(
					String.valueOf(engine.get(value2.getValue(),
							value3.getValue(), value1.getValue()))))));
		}
	}
}
