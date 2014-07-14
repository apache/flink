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

	private int windowSize;
	private int slidingStep;
	private int computeGranularity;
	private int windowFieldId;

	private WindowState<Integer> window;
	private MutableTableState<String, Integer> sum;

	private Integer number = 0;
	private Integer timestamp = 0;
	private StreamRecord outRecord = new StreamRecord(new Tuple2<Integer, Integer>());

	public WindowSumAggregate() {
		windowSize = 100;
		slidingStep = 20;
		computeGranularity = 10;
		windowFieldId = 1;
		window = new WindowState<Integer>(windowSize, slidingStep,
				computeGranularity, windowFieldId);
		sum = new MutableTableState<String, Integer>();
		sum.put("sum", 0);
	}

	private void incrementCompute(StreamRecord record) {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			number = record.getInteger(i, 0);
			sum.put("sum", sum.get("sum")+number);
		}
	}

	private void decrementCompute(StreamRecord record) {
		int numTuple = record.getNumOfTuples();
		for (int i = 0; i < numTuple; ++i) {
			number = record.getInteger(i, 0);
			sum.put("sum", sum.get("sum")-number);
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
				outRecord.setInteger(0, sum.get("sum"));
				outRecord.setInteger(1, record.getInteger(1));
				emit(outRecord);
			}
		} else {
			incrementCompute(record);
			window.pushBack(record);
			if (window.isFull()) {
				outRecord.setInteger(0, sum.get("sum"));
				outRecord.setInteger(1, record.getInteger(1));
				emit(outRecord);
			}
		}

	}
}
