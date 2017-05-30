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

package org.apache.flink.test.broadcastvars;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;

import org.junit.Assert;

import java.util.List;

/**
 * Test broadcast input after union.
 */
public class BroadcastUnionITCase extends JavaProgramTestBase {
	private static final String BC_NAME = "bc";

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<Long> input = env.generateSequence(1, 10);
		DataSet<Long> bc1 = env.generateSequence(1, 5);
		DataSet<Long> bc2 = env.generateSequence(6, 10);

		List<Long> result = input
				.map(new Mapper())
				.withBroadcastSet(bc1.union(bc2), BC_NAME)
				.reduce(new Reducer())
				.collect();

		Assert.assertEquals(Long.valueOf(3025), result.get(0));
	}

	private static class Mapper extends RichMapFunction<Long, Long> {
		private List<Long> values;

		@Override
		public void open(Configuration config) {
			values = getRuntimeContext().getBroadcastVariable(BC_NAME);
		}

		@Override
		public Long map(Long value) throws Exception {
			long sum = 0;
			for (Long v : values) {
				sum += value * v;
			}
			return sum;
		}
	}

	private static class Reducer implements ReduceFunction<Long> {
		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}
	}
}
