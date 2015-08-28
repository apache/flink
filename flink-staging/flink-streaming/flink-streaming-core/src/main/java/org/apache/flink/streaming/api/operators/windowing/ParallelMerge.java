/*
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
 */

package org.apache.flink.streaming.api.operators.windowing;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

/**
 * Class that encapsulates the functionality necessary to merge windows created
 * in parallel. This CoFlatMap uses the information received on the number of
 * parts for each window to merge the different parts. It waits until it
 * receives an indication on the number of parts from all the discretizers
 * before producing any output.
 */
public class ParallelMerge<OUT> extends
		RichCoFlatMapFunction<StreamWindow<OUT>, Tuple2<Integer, Integer>, StreamWindow<OUT>> {

	private static final long serialVersionUID = 1L;

	protected Integer numberOfDiscretizers;
	private ReduceFunction<OUT> reducer;

	private Map<Integer, Integer> availableNumberOfParts = new HashMap<Integer, Integer>();
	private Map<Integer, Tuple2<StreamWindow<OUT>, Integer>> receivedWindows = new HashMap<Integer, Tuple2<StreamWindow<OUT>, Integer>>();
	private Map<Integer, Tuple2<Integer, Integer>> receivedNumberOfParts = new HashMap<Integer, Tuple2<Integer, Integer>>();

	public ParallelMerge(ReduceFunction<OUT> reducer) {
		this.reducer = reducer;
	}

	@Override
	public void flatMap1(StreamWindow<OUT> nextWindow, Collector<StreamWindow<OUT>> out)
			throws Exception {

		Integer id = nextWindow.windowID;

		Tuple2<StreamWindow<OUT>, Integer> current = receivedWindows.get(id);

		if (current == null) {
			current = new Tuple2<StreamWindow<OUT>, Integer>(nextWindow, 1);
		} else {
			updateCurrent(current.f0, nextWindow);
			current.f1++;
		}

		Integer count = current.f1;

		if (availableNumberOfParts.containsKey(id) && availableNumberOfParts.get(id) <= count) {
			out.collect(current.f0);
			receivedWindows.remove(id);
			availableNumberOfParts.remove(id);

			checkOld(id);

		} else {
			receivedWindows.put(id, (Tuple2<StreamWindow<OUT>, Integer>) current);
		}
	}

	private void checkOld(Integer id) {
		// In case we have remaining partial windows (which indicates errors in
		// processing), output and log them
		if (receivedWindows.containsKey(id - 1)) {
			throw new RuntimeException("Error in processing logic, window with id " + id
					+ " should have already been processed");
		}

	}

	@Override
	public void flatMap2(Tuple2<Integer, Integer> partInfo, Collector<StreamWindow<OUT>> out)
			throws Exception {

		Integer id = partInfo.f0;
		Integer numOfParts = partInfo.f1;

		Tuple2<Integer, Integer> currentPartInfo = receivedNumberOfParts.get(id);
		if (currentPartInfo != null) {
			currentPartInfo.f0 += numOfParts;
			currentPartInfo.f1++;
		} else {
			currentPartInfo = new Tuple2<Integer, Integer>(numOfParts, 1);
			receivedNumberOfParts.put(id, currentPartInfo);
		}

		if (currentPartInfo.f1 >= numberOfDiscretizers) {
			receivedNumberOfParts.remove(id);

			Tuple2<StreamWindow<OUT>, Integer> current = receivedWindows.get(id);

			Integer count = current != null ? current.f1 : -1;

			if (count >= currentPartInfo.f0) {
				out.collect(current.f0);
				receivedWindows.remove(id);
				checkOld(id);
			} else if (currentPartInfo.f0 > 0) {
				availableNumberOfParts.put(id, currentPartInfo.f1);
			}
		}

	}

	protected void updateCurrent(StreamWindow<OUT> current, StreamWindow<OUT> nextWindow)
			throws Exception {
		if (current.size() != 1 || nextWindow.size() != 1) {
			throw new RuntimeException(
					"Error in parallel merge logic. Current window should contain only one element.");
		}
		OUT currentReduced = current.remove(0);
		currentReduced = reducer.reduce(currentReduced, nextWindow.get(0));
		current.add(currentReduced);
	}

	@Override
	public void open(Configuration conf) {
		this.numberOfDiscretizers = getRuntimeContext().getNumberOfParallelSubtasks();
	}

	Map<Integer, Tuple2<StreamWindow<OUT>, Integer>> getReceivedWindows() {
		return receivedWindows;
	}
}
