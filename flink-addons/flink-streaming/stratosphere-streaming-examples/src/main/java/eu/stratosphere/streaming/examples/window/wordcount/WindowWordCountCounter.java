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

import java.util.ArrayList;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.streaming.state.MutableTableState;
import eu.stratosphere.streaming.state.MutableTableStateIterator;
import eu.stratosphere.streaming.state.SlidingWindowState;
import eu.stratosphere.util.Collector;

public class WindowWordCountCounter extends
		FlatMapFunction<Tuple2<String, Long>, Tuple3<String, Integer, Long>> {
	private static final long serialVersionUID = 1L;

	private int windowSize = 10;
	private int slidingStep = 2;
	private int computeGranularity = 1;

	private ArrayList<Tuple2<String, Long>> tempTupleArray = null;
	private Tuple3<String, Integer, Long> outTuple = new Tuple3<String, Integer, Long>();
	private SlidingWindowState window;
	private MutableTableState<String, Integer> wordCounts;
	private long initTimestamp = -1;
	private long nextTimestamp = -1;
	private Long timestamp = 0L;

	public WindowWordCountCounter() {
		window = new SlidingWindowState(windowSize, slidingStep,
				computeGranularity);
		wordCounts = new MutableTableState<String, Integer>();
	}

	private void incrementCompute(ArrayList<Tuple2<String, Long>> tupleArray) {
		for (int i = 0; i < tupleArray.size(); ++i) {
			String word = tupleArray.get(i).f0;
			if (wordCounts.containsKey(word)) {
				int count = wordCounts.get(word) + 1;
				wordCounts.put(word, count);
			} else {
				wordCounts.put(word, 1);
			}
		}
	}

	private void decrementCompute(ArrayList<Tuple2<String, Long>> tupleArray) {
		for (int i = 0; i < tupleArray.size(); ++i) {
			String word = tupleArray.get(i).f0;
			int count = wordCounts.get(word) - 1;
			if (count == 0) {
				wordCounts.delete(word);
			} else {
				wordCounts.put(word, count);
			}
		}
	}

	private void produceOutput(long progress, Collector<Tuple3<String, Integer, Long>> out) {
		MutableTableStateIterator<String, Integer> iterator = wordCounts.getIterator();
		while (iterator.hasNext()) {
			Tuple2<String, Integer> tuple = iterator.next();
			outTuple.f0 = tuple.f0;
			outTuple.f1 = tuple.f1;
			outTuple.f2 = timestamp;
			out.collect(outTuple);
		}
	}

	@Override
	public void flatMap(Tuple2<String, Long> value,
			Collector<Tuple3<String, Integer, Long>> out) throws Exception {
		timestamp = value.f1;
		if (initTimestamp == -1) {
			initTimestamp = timestamp;
			nextTimestamp = initTimestamp + computeGranularity;
			tempTupleArray = new ArrayList<Tuple2<String, Long>>();
		} else {
			if (timestamp >= nextTimestamp) {
				if (window.isFull()) {
					ArrayList<Tuple2<String, Long>> expiredTupleArray = window.popFront();
					incrementCompute(tempTupleArray);
					decrementCompute(expiredTupleArray);
					window.pushBack(tempTupleArray);
					if (window.isEmittable()) {
						produceOutput(timestamp, out);
					}
				} else {
					incrementCompute(tempTupleArray);
					window.pushBack(tempTupleArray);
					if (window.isFull()) {
						produceOutput(timestamp, out);
					}
				}
				initTimestamp = nextTimestamp;
				nextTimestamp = initTimestamp + computeGranularity;
				tempTupleArray = new ArrayList<Tuple2<String, Long>>();
			}
		}
		tempTupleArray.add(value);		
	}
}
