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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.util.JavaProgramTestBase;

/**
 * Test iterations referenced from the static path of other iterations.
 */
public class StaticlyNestedIterationsITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Long> data1 = env.generateSequence(1, 100);
		DataSet<Long> data2 = env.generateSequence(1, 100);

		IterativeDataSet<Long> firstIteration = data1.iterate(100);

		DataSet<Long> firstResult = firstIteration.closeWith(firstIteration.map(new IdMapper()));

		IterativeDataSet<Long> mainIteration = data2.map(new IdMapper()).iterate(100);

		DataSet<Long> joined = mainIteration.join(firstResult)
				.where(new IdKeyExtractor()).equalTo(new IdKeyExtractor())
				.with(new Joiner());

		DataSet<Long> mainResult = mainIteration.closeWith(joined);

		mainResult.output(new DiscardingOutputFormat<Long>());

		env.execute();
	}

	private static class IdKeyExtractor implements KeySelector<Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(Long value) {
			return value;
		}
	}

	private static class IdMapper implements MapFunction<Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long value) {
			return value;
		}
	}

	private static class Joiner implements JoinFunction<Long, Long, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long join(Long first, Long second) {
			return first;
		}
	}
}
