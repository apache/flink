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

package eu.stratosphere.streaming.test.cellinfo;

import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;

public class CellTaskInvokable extends UserTaskInvokable {

	private WorkerEngineExact engine = new WorkerEngineExact(10, 500, System.currentTimeMillis());
	private StringValue outValue = new StringValue();
	IntValue cellID = new IntValue(0);
	LongValue timeStamp =new LongValue(0);
	IntValue lastMillis= new IntValue(0);
	
	StreamRecord outputRecord = new StreamRecord(outValue);
	
	int numOfFields;

	@Override
	public void invoke(StreamRecord record) throws Exception {
		
		cellID = (IntValue) record.getField(0);
		timeStamp = (LongValue) record.getField(1);

		numOfFields=record.getNumOfFields();
		// INFO
		if (numOfFields == 2) {
			engine.put(cellID.getValue(), timeStamp.getValue());
			outValue.setValue(cellID.toString() + " " + timeStamp.toString());
			outputRecord.setRecord(outValue);
			emit(outputRecord);
		}
		// QUERY
		else if (numOfFields == 3) {
			lastMillis = (IntValue) record.getField(2);
			outValue.setValue(String.valueOf(engine.get(timeStamp.getValue(),
					lastMillis.getValue(), cellID.getValue())));
			outputRecord.setRecord(outValue);
			emit(outputRecord);
		
		}
	}
}
