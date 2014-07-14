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

package eu.stratosphere.streaming.examples.window.wordcount;

import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.state.MutableInternalState;
import eu.stratosphere.streaming.state.WindowInternalState;

public class WindowWordCountCounter extends UserTaskInvokable {

	private int windowSize;
	private int slidingStep;

	private WindowInternalState<Integer> window;
	private MutableInternalState<String, Integer> wordCounts;

	private String word = "";
	private Integer count = 0;
	private Long timestamp = 0L;
	private StreamRecord outRecord = new StreamRecord(
			new Tuple3<String, Integer, Long>());

	public WindowWordCountCounter() {
		windowSize = 100;
		slidingStep = 20;
		window = new WindowInternalState<Integer>(windowSize, slidingStep);
		wordCounts = new MutableInternalState<String, Integer>();
	}

	private void incrementCompute(StreamRecord record) {
		word = record.getString(0);
		if (wordCounts.containsKey(word)) {
			count = wordCounts.get(word) + 1;
			wordCounts.put(word, count);
		} else {
			count = 1;
			wordCounts.put(word, 1);
		}
	}

	private void decrementCompute(StreamRecord record) {
		word = record.getString(0);
		count = wordCounts.get(word) - 1;
		if (count == 0) {
			wordCounts.delete(word);
		} else {
			wordCounts.put(word, count);
		}
	}

	@Override
	public void invoke(StreamRecord record) throws Exception {
		if (window.isFull()) {
			StreamRecord expiredRecord = window.popFront();
			incrementCompute(record);
			decrementCompute(expiredRecord);
			window.pushBack(record);
			if (window.isComputable()) {
				outRecord.setString(0, word);
				outRecord.setInteger(1, count);
				outRecord.setLong(2, timestamp);
				emit(outRecord);
			}
		} else {
			incrementCompute(record);
			window.pushBack(record);
			if(window.isFull()){
				outRecord.setString(0, word);
				outRecord.setInteger(1, count);
				outRecord.setLong(2, timestamp);
				emit(outRecord);
			}
		}

	}
}
