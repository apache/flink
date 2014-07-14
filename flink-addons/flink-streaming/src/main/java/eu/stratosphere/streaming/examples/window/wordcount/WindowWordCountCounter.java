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

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.MutableTableStateIterator;
import eu.stratosphere.streaming.state.WindowState;

public class WindowWordCountCounter extends UserTaskInvokable {

	private int windowSize=10;
	private int slidingStep=2;
	private int computeGranularity=1;
	private int windowFieldId=2;

	private StreamRecord tempRecord;
	private WindowState<Integer> window;
	private MutableTableState<String, Integer> wordCounts;
	private long initTimestamp=-1;
	private long nextTimestamp=-1;
	private Long timestamp = 0L;

	public WindowWordCountCounter() {
		window = new WindowState<Integer>(windowSize, slidingStep,
				computeGranularity);
		wordCounts = new MutableTableState<String, Integer>();
	}

	private void incrementCompute(StreamRecord record) {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			String word = record.getString(i, 0);
			if (wordCounts.containsKey(word)) {
				int count = wordCounts.get(word) + 1;
				wordCounts.put(word, count);
			} else {
				wordCounts.put(word, 1);
			}
		}
	}

	private void decrementCompute(StreamRecord record) {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			String word = record.getString(i, 0);
			int count = wordCounts.get(word) - 1;
			if (count == 0) {
				wordCounts.delete(word);
			} else {
				wordCounts.put(word, count);
			}
		}
	}

	private void produceRecord(long progress){
		StreamRecord outRecord = new StreamRecord(3);
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
	
	@Override
	public void invoke(StreamRecord record) throws Exception {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			long progress = record.getLong(i, windowFieldId);
			if (initTimestamp == -1) {
				initTimestamp = progress;
				nextTimestamp = initTimestamp + computeGranularity;
				tempRecord = new StreamRecord(record.getNumOfFields());
			} else {
				if (progress >= nextTimestamp) {
					if (window.isFull()) {
						StreamRecord expiredRecord = window.popFront();
						incrementCompute(tempRecord);
						decrementCompute(expiredRecord);
						window.pushBack(tempRecord);
						if (window.isEmittable()) {
							produceRecord(progress);
						}
					} else {
						incrementCompute(tempRecord);
						window.pushBack(tempRecord);
						if (window.isFull()) {
							produceRecord(progress);
						}
					}
					initTimestamp = nextTimestamp;
					nextTimestamp = initTimestamp + computeGranularity;
					tempRecord = new StreamRecord(record.getNumOfFields());
				}
			}
			tempRecord.addTuple(record.getTuple(i));
		}
	}
}
