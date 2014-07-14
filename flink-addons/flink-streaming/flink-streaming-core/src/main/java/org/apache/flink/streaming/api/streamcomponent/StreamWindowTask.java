/***********************************************************************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.api.streamcomponent;

import java.util.ArrayList;

import org.apache.flink.streaming.state.SlidingWindowState;
import org.apache.flink.streaming.state.StateManager;

import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;

public class StreamWindowTask<InTuple extends Tuple, OutTuple extends Tuple> extends FlatMapFunction<InTuple, OutTuple> {

	private int computeGranularity;
	private int windowFieldId;

	private ArrayList<InTuple> tempTupleArray;
	private SlidingWindowState<InTuple> window;
	private long initTimestamp = -1;
	private long nextTimestamp = -1;

	protected StateManager checkpointer = new StateManager("object.out", 1000);
	
	public StreamWindowTask(int windowSize, int slidingStep,
			int computeGranularity, int windowFieldId) {
		this.computeGranularity = computeGranularity;
		this.windowFieldId = windowFieldId;
		window = new SlidingWindowState<InTuple>(windowSize, slidingStep,
				computeGranularity);
		checkpointer.registerState(window);
		Thread t = new Thread(checkpointer);
		t.start();
	}

	protected void incrementCompute(ArrayList<InTuple> tupleArray) {}

	protected void decrementCompute(ArrayList<InTuple> tupleArray) {}

	protected void produceOutput(long progress, Collector<OutTuple> out) {}

	@Override
	public void flatMap(InTuple value, Collector<OutTuple> out)
			throws Exception {
		long progress = (Long) value.getField(windowFieldId);
		if (initTimestamp == -1) {
			initTimestamp = progress;
			nextTimestamp = initTimestamp + computeGranularity;
			tempTupleArray = new ArrayList<InTuple>();
		} else {
			if (progress > nextTimestamp) {
				if (window.isFull()) {
					ArrayList<InTuple> expiredTupleArray = window.popFront();
					incrementCompute(tempTupleArray);
					decrementCompute(expiredTupleArray);
					window.pushBack(tempTupleArray);
					if (window.isEmittable()) {
						produceOutput(progress, out);
					}
				} else {
					incrementCompute(tempTupleArray);
					window.pushBack(tempTupleArray);
					if (window.isFull()) {
						produceOutput(progress, out);
					}
				}
				initTimestamp = nextTimestamp;
				nextTimestamp = initTimestamp + computeGranularity;
				tempTupleArray = new ArrayList<InTuple>();
			}
			tempTupleArray.add(value);
		}		
	}
}
