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

import java.util.ArrayList;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.SlidingWindowState;
import eu.stratosphere.util.Collector;

public class WindowSumAggregate extends
		FlatMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>> {
	private static final long serialVersionUID = 1L;

	private int windowSize = 100;
	private int slidingStep = 20;
	private int computeGranularity = 10;

	private ArrayList<Tuple2<Integer, Long>> tempTupleArray = null;
	private Tuple2<Integer, Long> outTuple = new Tuple2<Integer, Long>();
	private SlidingWindowState window;
	private MutableTableState<String, Integer> sum;
	private long initTimestamp = -1;
	private long nextTimestamp = -1;

	public WindowSumAggregate() {
		window = new SlidingWindowState(windowSize, slidingStep,
				computeGranularity);
		sum = new MutableTableState<String, Integer>();
		sum.put("sum", 0);
	}

	private void incrementCompute(ArrayList<Tuple2<Integer, Long>> tupleArray) {
		for (int i = 0; i < tupleArray.size(); ++i) {
			int number = tupleArray.get(i).f0;
			sum.put("sum", sum.get("sum") + number);
		}
	}

	private void decrementCompute(ArrayList<Tuple2<Integer, Long>> tupleArray) {
		for (int i = 0; i < tupleArray.size(); ++i) {
			int number = tupleArray.get(i).f0;
			sum.put("sum", sum.get("sum") - number);
		}
	}

	private void produceOutput(long progress, Collector<Tuple2<Integer, Long>> out){
		outTuple.f0 = sum.get("sum");
		outTuple.f1 = progress;
		out.collect(outTuple);
	}
	
	@Override
	public void flatMap(Tuple2<Integer, Long> value,
			Collector<Tuple2<Integer, Long>> out) throws Exception {
		// TODO Auto-generated method stub
		long progress = value.f1;
		if (initTimestamp == -1) {
			initTimestamp = progress;
			nextTimestamp = initTimestamp + computeGranularity;
			tempTupleArray = new ArrayList<Tuple2<Integer, Long>>();
		} else {
			if (progress >= nextTimestamp) {
				if (window.isFull()) {
					ArrayList<Tuple2<Integer, Long>> expiredTupleArray = window.popFront();
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
				tempTupleArray = new ArrayList<Tuple2<Integer, Long>>();
			}
		}
		tempTupleArray.add(value);
	}
}
