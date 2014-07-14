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

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;

public class InfoSourceInvokable extends UserSourceInvokable {

	private final IntValue cellId1 = new IntValue(5);
	private final IntValue cellId2 = new IntValue(6);
	private final LongValue timeStamp = new LongValue(500);
	private final StreamRecord record1 = new StreamRecord(cellId1, timeStamp);
	private final StreamRecord record2 = new StreamRecord(cellId2, timeStamp);

	@Override
	public void invoke() throws Exception {
		for (int i = 0; i < 10; i++) {
			emit(record1);
			emit(record2);
		}
	}

}
