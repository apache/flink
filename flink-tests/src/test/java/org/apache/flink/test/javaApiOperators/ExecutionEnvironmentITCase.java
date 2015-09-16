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
package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * Test ExecutionEnvironment from user perspective
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class ExecutionEnvironmentITCase extends MultipleProgramsTestBase {
	private static final int PARALLELISM = 5;

	public ExecutionEnvironmentITCase(TestExecutionMode mode) {
		super(mode);
	}

	@Parameterized.Parameters(name = "Execution mode = {0}")
	public static Collection<TestExecutionMode[]> executionModes(){
		Collection<TestExecutionMode[]> c = new ArrayList<TestExecutionMode[]>(1);
		c.add(new TestExecutionMode[] {TestExecutionMode.CLUSTER});
		return c;
	}


	/**
	 * Ensure that the user can pass a custom configuration object to the LocalEnvironment
	 */
	@Test
	public void testLocalEnvironmentWithConfig() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, PARALLELISM);
		
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
