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

package eu.stratosphere.streaming.api.streamcomponent;

import java.util.ArrayList;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.SlidingWindowState;
import eu.stratosphere.util.Collector;

public class StreamWindowTask extends FlatMapFunction<Tuple, Tuple> {
	private static final long serialVersionUID = 1L;

	private int computeGranularity;
	private int windowFieldId = 1;

	private ArrayList tempArrayList;
	private SlidingWindowState<Integer> window;
	private MutableTableState<String, Integer> sum;
	private long initTimestamp = -1;
	private long nextTimestamp = -1;

	public StreamWindowTask(int windowSize, int slidingStep,
			int computeGranularity, int windowFieldId) {
		this.computeGranularity = computeGranularity;
		this.windowFieldId = windowFieldId;
		window = new SlidingWindowState<Integer>(windowSize, slidingStep,
				computeGranularity);
		sum = new MutableTableState<String, Integer>();
		sum.put("sum", 0);
	}

	private void incrementCompute(ArrayList tupleArray) {}

	private void decrementCompute(ArrayList tupleArray) {}

	private void produceOutput(long progress, Collector out) {}

	@Override
	public void flatMap(Tuple value, Collector<Tuple> out) throws Exception {
		// TODO Auto-generated method stub
		long progress = value.getField(windowFieldId);
		if (initTimestamp == -1) {
			initTimestamp = progress;
			nextTimestamp = initTimestamp + computeGranularity;
			tempArrayList = new ArrayList();
		} else {
			if (progress > nextTimestamp) {
				if (window.isFull()) {
					ArrayList expiredArrayList = window.popFront();
					incrementCompute(tempArrayList);
					decrementCompute(expiredArrayList);
					window.pushBack(tempArrayList);
					if (window.isEmittable()) {
						produceOutput(progress, out);
					}
				} else {
					incrementCompute(tempArrayList);
					window.pushBack(tempArrayList);
					if (window.isFull()) {
						produceOutput(progress, out);
					}
				}
				initTimestamp = nextTimestamp;
				nextTimestamp = initTimestamp + computeGranularity;
				tempArrayList = new ArrayList();
			}
			tempArrayList.add(value);
		}
	}
}
