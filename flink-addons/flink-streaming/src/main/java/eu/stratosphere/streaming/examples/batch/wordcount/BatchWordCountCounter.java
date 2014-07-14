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

package eu.stratosphere.streaming.examples.batch.wordcount;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.MutableTableStateIterator;

public class BatchWordCountCounter extends UserTaskInvokable {

	private MutableTableState<String, Integer> wordCounts = new MutableTableState<String, Integer>();
	private String word = "";
	private Integer count = 0;
	private Long timestamp = 0L;
	private StreamRecord outRecord = new StreamRecord(3);

	@Override
	public void invoke(StreamRecord record) throws Exception {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			word = record.getString(i, 0);
			count = record.getInteger(i, 1);
			timestamp = record.getLong(i, 2);
			if (wordCounts.containsKey(word)) {
				count = wordCounts.get(word) + 1;
				wordCounts.put(word, count);
			} else {
				count = 1;
				wordCounts.put(word, 1);
			}
		}

		MutableTableStateIterator<String, Integer> iterator = wordCounts
				.getIterator();
		while (iterator.hasNext()) {
			Tuple2<String, Integer> tuple = iterator.next();
			Tuple3<String, Integer, Long> outputTuple = new Tuple3<String, Integer, Long>(
					(String) tuple.getField(0), (Integer) tuple.getField(1), timestamp);
			outRecord.addTuple(outputTuple);
		}
		emit(outRecord);
	}
}