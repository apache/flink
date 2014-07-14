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
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;

public class TestSourceInvokable extends UserSourceInvokable {

	@Override
	public void invoke() throws Exception {
		for (int i = 0; i < 10; i++) {
			StreamRecord batch1 = new StreamRecord(2);
			AtomRecord record1 = new AtomRecord(2);
			record1.setField(0, new IntValue(5));
			record1.setField(1, new LongValue(500));
			
			StreamRecord batch2 = new StreamRecord(2);
			AtomRecord record2 = new AtomRecord(2);
			record2.setField(0, new IntValue(4));
			record2.setField(1, new LongValue(500));
			
			emit(batch1);
			emit(batch2);
		}
	}

}
