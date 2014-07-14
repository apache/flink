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

package eu.stratosphere.streaming.test.batch;

import eu.stratosphere.streaming.api.AtomRecord;
import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.StringValue;

public class MyBatchStreamSource  extends UserSourceInvokable {
//	private final String motto = "Stratosphere Big Data looks tiny from here";
	private final String motto = "Gyuszi Gabor Big Marci Gyuszi";
	private final AtomRecord record=new AtomRecord();
	private final StreamRecord mottoRecord=new StreamRecord();
	
	@Override
	public void invoke() throws Exception {
		record.setField(0, new StringValue(motto));
		mottoRecord.addRecord(record);
		for (int i = 0; i < 100; i++) {
			emit(mottoRecord);
		}
	}
}