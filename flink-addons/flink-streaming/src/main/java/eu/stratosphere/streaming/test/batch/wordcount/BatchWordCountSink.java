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

package eu.stratosphere.streaming.test.batch.wordcount;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;

public class BatchWordCountSink implements UserSinkInvokable {

	private StringValue word = new StringValue();
	private IntValue count = new IntValue();
	private LongValue timestamp = new LongValue();

	@Override
	public void invoke(StreamRecord record) throws Exception {
		word = (StringValue) record.getField(0, 0);
		count = (IntValue) record.getField(0, 1);
		timestamp = (LongValue) record.getField(0, 2);
		System.out.println("============================================");
		System.out.println(word.getValue() + " " + count.getValue() + " "
				+ timestamp.getValue());
		System.out.println("============================================");

	}
}
