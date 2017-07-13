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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.optimizer.iterations.MultipleJoinsWithSolutionSetCompilerTest;
import org.apache.flink.test.util.JavaProgramTestBase;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Test multiple joins with the solution set.
 */
public class MultipleSolutionSetJoinsITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {

		final int numIters = 4;
		final double expectedFactor = (int) Math.pow(7, numIters);

		// this is an artificial program, it does not compute anything sensical
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Long, Double>> initialData = env.fromElements(new Tuple2<Long, Double>(1L, 1.0), new Tuple2<Long, Double>(2L, 2.0),
															new Tuple2<Long, Double>(3L, 3.0), new Tuple2<Long, Double>(4L, 4.0),
															new Tuple2<Long, Double>(5L, 5.0), new Tuple2<Long, Double>(6L, 6.0));

		DataSet<Tuple2<Long, Double>> result = MultipleJoinsWithSolutionSetCompilerTest.constructPlan(initialData, numIters);

		List<Tuple2<Long, Double>> resultCollector = new ArrayList<Tuple2<Long, Double>>();
		result.output(new LocalCollectionOutputFormat<>(resultCollector));

		env.execute();

		for (Tuple2<Long, Double> tuple : resultCollector) {
			Assert.assertEquals(expectedFactor * tuple.f0, tuple.f1.doubleValue(), 0.0);
		}
	}
}
