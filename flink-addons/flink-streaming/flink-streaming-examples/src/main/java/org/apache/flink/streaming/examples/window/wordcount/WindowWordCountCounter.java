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

package org.apache.flink.streaming.examples.window.wordcount;

import java.util.ArrayList;

import org.apache.flink.streaming.api.streamcomponent.StreamWindowTask;
import org.apache.flink.streaming.state.TableState;
import org.apache.flink.streaming.state.TableStateIterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WindowWordCountCounter extends
		StreamWindowTask<Tuple2<String, Long>, Tuple3<String, Integer, Long>> {
	private static final long serialVersionUID = 1L;

	private Tuple3<String, Integer, Long> outTuple = new Tuple3<String, Integer, Long>();
	private TableState<String, Integer> wordCounts;

	public WindowWordCountCounter(int windowSize, int slidingStep,
			int computeGranularity, int windowFieldId) {
		super(windowSize, slidingStep, computeGranularity, windowFieldId);
		wordCounts = new TableState<String, Integer>();
	}
	
	@Override
	protected void incrementCompute(ArrayList<Tuple2<String, Long>> tupleArray) {
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

	@Override
	protected void decrementCompute(ArrayList<Tuple2<String, Long>> tupleArray) {
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

	@Override
	protected void produceOutput(long progress, Collector<Tuple3<String, Integer, Long>> out) {
		TableStateIterator<String, Integer> iterator = wordCounts.getIterator();
		while (iterator.hasNext()) {
			Tuple2<String, Integer> tuple = iterator.next();
			outTuple.f0 = tuple.f0;
			outTuple.f1 = tuple.f1;
			outTuple.f2 = progress;
			out.collect(outTuple);
		}
	}
}
