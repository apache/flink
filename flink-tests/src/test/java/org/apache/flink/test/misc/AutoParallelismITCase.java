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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResource.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterResource.MiniClusterType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test verifies that the auto parallelism is properly forwarded to the runtime.
 */
@SuppressWarnings("serial")
public class AutoParallelismITCase extends TestLogger {

	private static final int NUM_TM = 2;
	private static final int SLOTS_PER_TM = 7;
	private static final int PARALLELISM = NUM_TM * SLOTS_PER_TM;

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration(
			new Configuration(),
			NUM_TM,
			SLOTS_PER_TM));

	@Test
	public void testProgramWithAutoParallelism() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(ExecutionConfig.PARALLELISM_AUTO_MAX);
		env.getConfig().disableSysoutLogging();

		DataSet<Integer> result = env
				.createInput(new ParallelismDependentInputFormat())
				.rebalance()
				.mapPartition(new ParallelismDependentMapPartition());

		List<Integer> resultCollection = new ArrayList<>();
		result.output(new LocalCollectionOutputFormat<>(resultCollection));

		try {
			env.execute();
			assertEquals(PARALLELISM, resultCollection.size());
		}
		catch (Exception ex) {
			if (MINI_CLUSTER_RESOURCE.getMiniClusterType().equals(MiniClusterType.LEGACY)) {
				throw ex;
			}
			assertTrue(
				ExceptionUtils.findThrowableWithMessage(ex, ExecutionGraphBuilder.PARALLELISM_AUTO_MAX_ERROR_MESSAGE).isPresent());
		}
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

	private static class ParallelismDependentMapPartition extends RichMapPartitionFunction<Integer, Integer> {

		@Override
		public void mapPartition(Iterable<Integer> values, Collector<Integer> out) {
			out.collect(getRuntimeContext().getIndexOfThisSubtask());
		}
	}
}
