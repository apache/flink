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

package eu.stratosphere.streaming.examples.window.sum;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.WindowState;

public class WindowSumAggregate extends UserTaskInvokable {

	private int windowSize = 100;
	private int slidingStep = 20;
	private int computeGranularity = 10;
	private int windowFieldId = 1;

	private StreamRecord tempRecord;
	private WindowState<Integer> window;
	private MutableTableState<String, Integer> sum;
	private long initTimestamp = -1;
	private long nextTimestamp = -1;

	private StreamRecord outRecord = new StreamRecord(
			new Tuple2<Integer, Long>());

	public WindowSumAggregate() {
		window = new WindowState<Integer>(windowSize, slidingStep,
				computeGranularity);
		sum = new MutableTableState<String, Integer>();
		sum.put("sum", 0);
	}

	private void incrementCompute(StreamRecord record) {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			int number = record.getInteger(i, 0);
			sum.put("sum", sum.get("sum") + number);
		}
	}

	private void decrementCompute(StreamRecord record) {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			int number = record.getInteger(i, 0);
			sum.put("sum", sum.get("sum") - number);
		}
	}
	
	private void produceRecord(long progress){
		outRecord.setInteger(0, sum.get("sum"));
		outRecord.setLong(1, progress);
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
				if (progress > nextTimestamp) {
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
				tempRecord.addTuple(record.getTuple(i));
			}
		}
	}
}
