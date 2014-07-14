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

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;

public class BatchWordCountCounter extends UserTaskInvokable {
	
	private int windowSize = 100;
	private int slidingStep = 20;
	
	private Map<String, Integer> wordCounts = new HashMap<String, Integer>();
	private StringValue wordValue = new StringValue("");
	private IntValue countValue = new IntValue(1);
	private LongValue timestamp = new LongValue(0);
	private String word = "";
	private StreamRecord outputRecord = new StreamRecord(3);
	private int count = 1;

	@Override
	public void invoke(StreamRecord record) throws Exception {
		wordValue=(StringValue) record.getField(0);
		timestamp=(LongValue) record.getField(1);

		if (wordCounts.containsKey(word)) {
			count = wordCounts.get(word) + 1;
			wordCounts.put(word, count);
			countValue.setValue(count);
		} else {
			wordCounts.put(word, 1);
			countValue.setValue(1);
		}
		outputRecord.setField(0, wordValue);
		outputRecord.setField(1, countValue);
		outputRecord.setField(2, timestamp);
		emit(outputRecord);

	}
}