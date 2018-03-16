/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.accumulators;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;

/**
 * Test accumulator within iteration.
 */
public class AccumulatorIterativeITCase extends JavaProgramTestBase {
	private static final int NUM_ITERATIONS = 3;
	private static final int NUM_SUBTASKS = 1;
	private static final String ACC_NAME = "test";

	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(NUM_SUBTASKS);

		IterativeDataSet<Integer> iteration = env.fromElements(1, 2, 3).iterate(NUM_ITERATIONS);

		iteration.closeWith(iteration.reduceGroup(new SumReducer())).output(new DiscardingOutputFormat<Integer>());

		Assert.assertEquals(NUM_ITERATIONS * 6, (int) env.execute().getAccumulatorResult(ACC_NAME));
	}

	static final class SumReducer extends RichGroupReduceFunction<Integer, Integer> {

		private static final long serialVersionUID = 1L;

		private IntCounter testCounter = new IntCounter();

		@Override
		public void open(Configuration config) throws Exception {
			getRuntimeContext().addAccumulator(ACC_NAME, this.testCounter);
		}

		@Override
		public void reduce(Iterable<Integer> values, Collector<Integer> out) {
			// Compute the sum
			int sum = 0;

			for (Integer value : values) {
				sum += value;
				testCounter.add(value);
			}
			out.collect(sum);
		}
	}
}
