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

package eu.stratosphere.streaming.test.wordcount;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class WordCountDummySource extends UserSourceInvokable {

	private String line = new String();
	private StringValue lineValue = new StringValue();
	private Value[] values = new Value[1];

	public WordCountDummySource() {

	}

	@Override
	public void invoke() throws Exception {
		line = "first one";
		lineValue.setValue(line);
		values[0] = lineValue;
		StreamRecord record = new StreamRecord(lineValue);
		System.out.println(record.getNumOfRecords());

		record.copy();
		emit(record);

		line = "second two";
		lineValue.setValue(line);
		values[0] = lineValue;
		record.setRecord(0, values);

		emit(record);
	}
}
