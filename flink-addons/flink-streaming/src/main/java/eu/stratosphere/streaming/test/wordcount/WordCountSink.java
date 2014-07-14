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

import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;

public class WordCountSink implements UserSinkInvokable {

	private Map<String, Integer> wordCounts = new HashMap<String, Integer>();
	private StringValue word = new StringValue("");

	@Override
	public void invoke(Record record) throws Exception {

		record.getFieldInto(0, word);

		if (wordCounts.containsKey(word.getValue())) {
			wordCounts
					.put(word.getValue(), wordCounts.get(word.getValue()) + 1);
			System.out.println(word.getValue() + " "
					+ wordCounts.get(word.getValue()));
		} else {
			wordCounts.put(word.getValue(), 1);
			System.out.println(word.getValue() + " "
					+ wordCounts.get(word.getValue()));

		}
	}

}