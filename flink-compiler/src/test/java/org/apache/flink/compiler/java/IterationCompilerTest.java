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

package org.apache.flink.compiler.java;

import static org.junit.Assert.*;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.compiler.CompilerTestBase;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plandump.PlanJSONDumpGenerator;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.junit.Test;

@SuppressWarnings("serial")
public class IterationCompilerTest extends CompilerTestBase {

	@Test
	public void testIdentityIteration() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(43);
			
			IterativeDataSet<Long> iteration = env.generateSequence(-4, 1000).iterate(100);
			iteration.closeWith(iteration).print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			new NepheleJobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testIdentityWorksetIteration() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(43);
			
			DataSet<Tuple2<Long, Long>> input = env.generateSequence(1, 20)
					.map(new MapFunction<Long, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> map(Long value){ return null; }
					});
					
					
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iter = input.iterateDelta(input, 100, 0);
			iter.closeWith(iter.getWorkset(), iter.getWorkset())
				.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			System.out.println(new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(op));
			
			new NepheleJobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
