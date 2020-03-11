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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.utils.PlanGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test ExecutionEnvironment from user perspective.
 */
@SuppressWarnings("serial")
public class ExecutionEnvironmentITCase extends TestLogger {

	private static final int PARALLELISM = 5;

	/**
	 * Ensure that the user can pass a custom configuration object to the LocalEnvironment.
	 */
	@Test
	public void testLocalEnvironmentWithConfig() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, PARALLELISM);

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);

		DataSet<Integer> result = env.createInput(new ParallelismDependentInputFormat())
				.rebalance()
				.mapPartition(new RichMapPartitionFunction<Integer, Integer>() {
					@Override
					public void mapPartition(Iterable<Integer> values, Collector<Integer> out) throws Exception {
						out.collect(getRuntimeContext().getIndexOfThisSubtask());
					}
				});
		List<Integer> resultCollection = result.collect();
		assertEquals(PARALLELISM, resultCollection.size());
	}

	private static class ParallelismDependentInputFormat extends GenericInputFormat<Integer> {

		private transient boolean emitted;

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
			assertEquals(PARALLELISM, numSplits);
			return super.createInputSplits(numSplits);
		}

		@Override
		public boolean reachedEnd() {
			return emitted;
		}

		@Override
		public Integer nextRecord(Integer reuse) {
			if (emitted) {
				return null;
			}
			emitted = true;
			return 1;
		}
	}

	@Test
	public void testExecuteGivenPlan() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSet<String> dataSet = env
				.fromElements(1, 3, 5)
				.map((MapFunction<Integer, String>) value -> String.valueOf(value + 1));

		TypeSerializer<String> serializer = dataSet.getType().createSerializer(env.getConfig());
		Utils.CollectHelper<String> format = new Utils.CollectHelper<>("id", serializer);
		DataSink<?> sink = dataSet.output(format);
		PlanGenerator generator = new PlanGenerator(
				Collections.singletonList(sink), env.getConfig(), env.getCacheFile(), "test");
		Plan plan = generator.generate();
		// execute given plan
		JobExecutionResult result = env.execute(plan);

		ArrayList<byte[]> accResult = result.getJobExecutionResult().getAccumulatorResult("id");
		List<String> list = SerializedListAccumulator.deserializeList(accResult, serializer);
		assertEquals(Arrays.asList("2", "4", "6"), list);
	}
}
