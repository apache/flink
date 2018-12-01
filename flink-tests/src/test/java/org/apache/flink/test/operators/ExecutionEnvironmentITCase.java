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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test ExecutionEnvironment from user perspective.
 */
@SuppressWarnings("serial")
public class ExecutionEnvironmentITCase extends TestLogger {

	private static final int PARALLELISM = 5;

	private static class TestOutputFormat implements OutputFormat {

		private static Map<String, Integer> countMap = new HashMap<>();

		private String name;

		public TestOutputFormat(String name) {
			this.name = name;
		}

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			if (countMap.containsKey(name)) {
				countMap.put(name, countMap.get(name) + 1);
			} else {
				countMap.put(name, 1);
			}
		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public void writeRecord(Object record) throws IOException {

		}

		public int getOpenCount() {
			if (!countMap.containsKey(name)) {
				return 0;
			} else {
				return countMap.get(name);
			}
		}
	}

	@Test
	public void testPartialDAG() throws Exception {
		ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
		benv.setParallelism(1);

		List<String> sourceData = new ArrayList<>();
		sourceData.add("hello world");
		sourceData.add("hello flink");
		sourceData.add("hello hadoop");

		DataSet<String> data = benv.fromCollection(sourceData);
		TestOutputFormat outputFormat1 = new TestOutputFormat("sink1");
		TestOutputFormat outputFormat2 = new TestOutputFormat("sink2");
		DataSink sink1 = data.output(outputFormat1);
		DataSink sink2 = data.output(outputFormat2);

		benv.execute(sink1);
		Assert.assertEquals(1, outputFormat1.getOpenCount());
		Assert.assertEquals(0, outputFormat2.getOpenCount());

		benv.execute();
		Assert.assertEquals(1, outputFormat1.getOpenCount());
		Assert.assertEquals(1, outputFormat2.getOpenCount());

		try {
			benv.execute();
			Assert.fail("Should fail to run execute again");
		} catch (Exception e) {
			Assert.assertTrue(e.getMessage()
				.contains("No new data sinks have been defined since the last execution"));
		}
	}

	/**
	 * Ensure that the user can pass a custom configuration object to the LocalEnvironment.
	 */
	@Test
	public void testLocalEnvironmentWithConfig() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, PARALLELISM);

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
		env.setParallelism(ExecutionConfig.PARALLELISM_AUTO_MAX);
		env.getConfig().disableSysoutLogging();

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
}
