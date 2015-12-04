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

package org.apache.flink.optimizer.plantranslate;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TempInIterationsTest {

	/*
	 * Tests whether temps barriers are correctly set in within iterations
	 */
	@Test
	public void testTempInIterationTest() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> input = env.readCsvFile("file:///does/not/exist").types(Long.class, Long.class);

		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				input.iterateDelta(input, 1, 0);

		DataSet<Tuple2<Long, Long>> update = iteration.getWorkset()
				.join(iteration.getSolutionSet()).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

		iteration.closeWith(update, update)
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());


		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = (new Optimizer(new Configuration())).compile(plan);

		JobGraphGenerator jgg = new JobGraphGenerator();
		JobGraph jg = jgg.compileJobGraph(oPlan);

		boolean solutionSetUpdateChecked = false;
		for(JobVertex v : jg.getVertices()) {
			if(v.getName().equals("SolutionSet Delta")) {

				// check if input of solution set delta is temped
				TaskConfig tc = new TaskConfig(v.getConfiguration());
				assertTrue(tc.isInputAsynchronouslyMaterialized(0));
				solutionSetUpdateChecked = true;
			}
		}
		assertTrue(solutionSetUpdateChecked);

	}

}
