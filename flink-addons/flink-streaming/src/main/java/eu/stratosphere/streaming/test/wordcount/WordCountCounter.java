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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import eu.stratosphere.streaming.api.StreamRecord;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

public class WordCountCounter extends UserTaskInvokable {

	private Map<String, Integer> wordCounts = new HashMap<String, Integer>();
	private StringValue wordValue = new StringValue("");
	private IntValue countValue = new IntValue(1);
	private String word = "";
	private StreamRecord outputRecord = new StreamRecord(2);
	private int count = 1;

	@Override
	public void invoke(StreamRecord record) throws Exception {
		wordValue = (StringValue) record.getField(0);
		word = wordValue.getValue();
		
		if (wordCounts.containsKey(word)) {
			count = wordCounts.get(word) + 1;
			wordCounts.put(word, count);
			countValue.setValue(count);
			outputRecord.setField(0, wordValue);
			outputRecord.setField(1, countValue);
			emit(outputRecord);
		} else {
			wordCounts.put(word, 1);
			countValue.setValue(1);
			outputRecord.setField(0, wordValue);
			outputRecord.setField(1, countValue);
			emit(outputRecord);
		}

	}
}
