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

package eu.stratosphere.streaming.examples.cellinfo;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class CellTask extends UserTaskInvokable {

	private WorkerEngineExact engine = new WorkerEngineExact(10, 500,
			System.currentTimeMillis());
	Integer cellID;
	Long timeStamp;
	Integer lastMillis;

	StreamRecord outputRecord = new StreamRecord(new Tuple1<String>());

	int numOfFields;

	@Override
	public void invoke(StreamRecord record) throws Exception {

		cellID = record.getInteger(0);
		timeStamp = record.getLong(1);
		numOfFields = record.getNumOfFields();

		// TODO: consider adding source to StreamRecord as a workaround
		// INFO
		if (numOfFields == 2) {
			engine.put(cellID, timeStamp);
			outputRecord.setString(0, cellID + " " + timeStamp);
			emit(outputRecord);
		}
		// QUERY
		else if (numOfFields == 3) {
			lastMillis = record.getInteger(2);
			outputRecord.setString(0,
					String.valueOf(engine.get(timeStamp, lastMillis, cellID)));
			emit(outputRecord);

		}
	}
}
