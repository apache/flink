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

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.streamcomponent.StreamWindowTask;
import eu.stratosphere.streaming.state.TableState;
import eu.stratosphere.util.Collector;

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
		//checkpointer.RegisterState(sum);
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
