/**
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
 */

package org.apache.flink.streaming.examples.window.sum;

import java.util.ArrayList;

import org.apache.flink.streaming.api.streamcomponent.StreamWindowTask;
import org.apache.flink.streaming.state.TableState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WindowSumAggregate extends
		StreamWindowTask<Tuple2<Integer, Long>, Tuple2<Integer, Long>> {
	private static final long serialVersionUID = -2832409561059237150L;
	private TableState<String, Integer> sum;
	private Tuple2<Integer, Long> outTuple = new Tuple2<Integer, Long>();
	
	
	public WindowSumAggregate(int windowSize, int slidingStep,
			int computeGranularity, int windowFieldId) {
		super(windowSize, slidingStep, computeGranularity, windowFieldId);
		sum = new TableState<String, Integer>();
		sum.put("sum", 0);
		checkpointer.registerState(sum);
	}

	@Override
	protected void incrementCompute(ArrayList<Tuple2<Integer, Long>> tupleArray) {
		for (int i = 0; i < tupleArray.size(); ++i) {
			int number = tupleArray.get(i).f0;
			sum.put("sum", sum.get("sum") + number);
		}
	}

	@Override
	protected void decrementCompute(ArrayList<Tuple2<Integer, Long>> tupleArray) {
		for (int i = 0; i < tupleArray.size(); ++i) {
			int number = tupleArray.get(i).f0;
			sum.put("sum", sum.get("sum") - number);
		}
	}

	@Override
	protected void produceOutput(long progress, Collector<Tuple2<Integer, Long>> out){
		outTuple.f0 = sum.get("sum");
		outTuple.f1 = progress;
		out.collect(outTuple);
	}
}
