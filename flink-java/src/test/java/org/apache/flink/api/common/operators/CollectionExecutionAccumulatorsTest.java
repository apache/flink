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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link CollectionExecutor} with accumulators.
 */
public class CollectionExecutionAccumulatorsTest {

	private static final String ACCUMULATOR_NAME = "TEST ACC";

	@Test
	public void testAccumulator() {
		try {
			final int numElements = 100;

			ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

			env.generateSequence(1, numElements)
				.map(new CountingMapper())
				.output(new DiscardingOutputFormat<Long>());

			JobExecutionResult result = env.execute();

			assertTrue(result.getNetRuntime() >= 0);

			assertEquals(numElements, (int) result.getAccumulatorResult(ACCUMULATOR_NAME));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@SuppressWarnings("serial")
	private static class CountingMapper extends RichMapFunction<Long, Long> {

		private IntCounter accumulator;

		@Override
		public void open(Configuration parameters) {
			accumulator = getRuntimeContext().getIntCounter(ACCUMULATOR_NAME);
		}

		@Override
		public Long map(Long value) {
			accumulator.add(1);
			return value;
		}
	}
}
