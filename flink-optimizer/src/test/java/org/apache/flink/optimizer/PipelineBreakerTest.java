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

package org.apache.flink.optimizer;

import static org.junit.Assert.*;

import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.configuration.Configuration;

@SuppressWarnings("serial")
public class PipelineBreakerTest extends CompilerTestBase {

	@Test
	public void testPipelineBreakerWithBroadcastVariable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(64);
			
			DataSet<Long> source = env.generateSequence(1, 10).map(new IdentityMapper<Long>());
			
			DataSet<Long> result = source.map(new IdentityMapper<Long>())
										.map(new IdentityMapper<Long>())
											.withBroadcastSet(source, "bc");
			
			result.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
			
			assertTrue(mapper.getInput().getTempMode().breaksPipeline());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testPipelineBreakerBroadcastedAllReduce() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(64);
			
			DataSet<Long> sourceWithMapper = env.generateSequence(1, 10).map(new IdentityMapper<Long>());
			
			DataSet<Long> bcInput1 = sourceWithMapper
										.map(new IdentityMapper<Long>())
										.reduce(new SelectOneReducer<Long>());
			DataSet<Long> bcInput2 = env.generateSequence(1, 10);
			
			DataSet<Long> result = sourceWithMapper
					.map(new IdentityMapper<Long>())
							.withBroadcastSet(bcInput1, "bc1")
							.withBroadcastSet(bcInput2, "bc2");
			
			result.print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
			
			assertTrue(mapper.getInput().getTempMode().breaksPipeline());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testPipelineBreakerBroadcastedPartialSolution() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(64);
			
			
			DataSet<Long> initialSource = env.generateSequence(1, 10);
			IterativeDataSet<Long> iteration = initialSource.iterate(100);
			
			
			DataSet<Long> sourceWithMapper = env.generateSequence(1, 10).map(new IdentityMapper<Long>());
			
			DataSet<Long> bcInput1 = sourceWithMapper
										.map(new IdentityMapper<Long>())
										.reduce(new SelectOneReducer<Long>());
			
			DataSet<Long> result = sourceWithMapper
					.map(new IdentityMapper<Long>())
							.withBroadcastSet(iteration, "bc2")
							.withBroadcastSet(bcInput1, "bc1");
							
			
			iteration.closeWith(result).print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			BulkIterationPlanNode iterationPlanNode = (BulkIterationPlanNode) sink.getInput().getSource();
			SingleInputPlanNode mapper = (SingleInputPlanNode) iterationPlanNode.getRootOfStepFunction();
			
			assertTrue(mapper.getInput().getTempMode().breaksPipeline());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testPilelineBreakerWithCross() {
		try {
			{
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(64);
				
				DataSet<Long> initialSource = env.generateSequence(1, 10);
				
				Configuration conf= new Configuration();
				conf.setString(Optimizer.HINT_LOCAL_STRATEGY, Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST);
				initialSource
					.map(new IdentityMapper<Long>())
					.cross(initialSource).withParameters(conf)
					.print();
				
				
				Plan p = env.createProgramPlan();
				OptimizedPlan op = compileNoStats(p);
				SinkPlanNode sink = op.getDataSinks().iterator().next();
				DualInputPlanNode mapper = (DualInputPlanNode) sink.getInput().getSource();
				
				assertTrue(mapper.getInput1().getTempMode().breaksPipeline());
			}
			
			{
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(64);
				
				DataSet<Long> initialSource = env.generateSequence(1, 10);
				
				Configuration conf= new Configuration();
				conf.setString(Optimizer.HINT_LOCAL_STRATEGY, Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND);
				initialSource
					.map(new IdentityMapper<Long>())
					.cross(initialSource).withParameters(conf)
					.print();
				
				
				Plan p = env.createProgramPlan();
				OptimizedPlan op = compileNoStats(p);
				
				SinkPlanNode sink = op.getDataSinks().iterator().next();
				DualInputPlanNode mapper = (DualInputPlanNode) sink.getInput().getSource();
				
				assertTrue(mapper.getInput2().getTempMode().breaksPipeline());
			}
			
			{
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(64);
				
				DataSet<Long> initialSource = env.generateSequence(1, 10);
				
				Configuration conf= new Configuration();
				conf.setString(Optimizer.HINT_LOCAL_STRATEGY, Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST);
				initialSource
					.map(new IdentityMapper<Long>())
					.cross(initialSource).withParameters(conf)
					.print();
				
				
				Plan p = env.createProgramPlan();
				OptimizedPlan op = compileNoStats(p);
				
				SinkPlanNode sink = op.getDataSinks().iterator().next();
				DualInputPlanNode mapper = (DualInputPlanNode) sink.getInput().getSource();
				
				assertTrue(mapper.getInput1().getTempMode().breaksPipeline());
			}
			
			{
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(64);
				
				DataSet<Long> initialSource = env.generateSequence(1, 10);
				
				Configuration conf= new Configuration();
				conf.setString(Optimizer.HINT_LOCAL_STRATEGY, Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND);
				initialSource
					.map(new IdentityMapper<Long>())
					.cross(initialSource).withParameters(conf)
					.print();
				
				
				Plan p = env.createProgramPlan();
				OptimizedPlan op = compileNoStats(p);
				
				SinkPlanNode sink = op.getDataSinks().iterator().next();
				DualInputPlanNode mapper = (DualInputPlanNode) sink.getInput().getSource();
				
				assertTrue(mapper.getInput2().getTempMode().breaksPipeline());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
