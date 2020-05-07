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

package org.apache.flink.client.program;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Tests for the generation of execution plans.
 */
public class ExecutionPlanCreationTest {

	@Test
	public void testGetExecutionPlan() {
		try {
			PackagedProgram prg = PackagedProgram.newBuilder()
				.setEntryPointClassName(TestOptimizerPlan.class.getName())
				.setArguments("/dev/random", "/tmp")
				.build();

			InetAddress mockAddress = InetAddress.getLocalHost();
			InetSocketAddress mockJmAddress = new InetSocketAddress(mockAddress, 12345);

			Configuration config = new Configuration();

			config.setString(JobManagerOptions.ADDRESS, mockJmAddress.getHostName());
			config.setInteger(JobManagerOptions.PORT, mockJmAddress.getPort());

			Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), config);
			Plan plan = (Plan) PackagedProgramUtils.getPipelineFromProgram(prg, config, -1, true);
			OptimizedPlan op = optimizer.compile(plan);
			assertNotNull(op);

			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			assertNotNull(dumper.getOptimizerPlanAsJSON(op));

			// test HTML escaping
			PlanJSONDumpGenerator dumper2 = new PlanJSONDumpGenerator();
			dumper2.setEncodeForHTML(true);
			String htmlEscaped = dumper2.getOptimizerPlanAsJSON(op);

			assertEquals(-1, htmlEscaped.indexOf('\\'));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * A test job.
	 */
	public static class TestOptimizerPlan implements ProgramDescription {

		@SuppressWarnings("serial")
		public static void main(String[] args) throws Exception {
			if (args.length < 2) {
				System.err.println("Usage: TestOptimizerPlan <input-file-path> <output-file-path>");
				return;
			}

			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> input = env.readCsvFile(args[0])
					.fieldDelimiter("\t").types(Long.class, Long.class);

			DataSet<Tuple2<Long, Long>> result = input.map(
					new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
						public Tuple2<Long, Long> map(Tuple2<Long, Long> value){
							return new Tuple2<Long, Long>(value.f0, value.f1 + 1);
						}
			});
			result.writeAsCsv(args[1], "\n", "\t");
			env.execute();
		}

		@Override
		public String getDescription() {
			return "TestOptimizerPlan <input-file-path> <output-file-path>";
		}
	}
}
