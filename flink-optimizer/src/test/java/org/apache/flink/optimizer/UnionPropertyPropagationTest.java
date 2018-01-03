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

import java.util.Iterator;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.base.FlatMapOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Visitor;
import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;


@SuppressWarnings({"serial"})
public class UnionPropertyPropagationTest extends CompilerTestBase {

	@Test
	public void testUnion1() {
		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);
		DataSet<Long> sourceA = env.generateSequence(0,1);
		DataSet<Long> sourceB = env.generateSequence(0,1);

		DataSet<Long> redA = sourceA.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>());
		DataSet<Long> redB = sourceB.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>());

		redA.union(redB).groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
			.output(new DiscardingOutputFormat<Long>());

		Plan plan = env.createProgramPlan();
		
		OptimizedPlan oPlan = compileNoStats(plan);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		// Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof SingleInputPlanNode && visitable.getProgramOperator() instanceof GroupReduceOperatorBase) {
					for (Channel inConn : visitable.getInputs()) {
						Assert.assertTrue("Reduce should just forward the input if it is already partitioned",
								inConn.getShipStrategy() == ShipStrategyType.FORWARD); 
					}
					//just check latest ReduceNode
					return false;
				}
				return true;
			}
			
			@Override
			public void postVisit(PlanNode visitable) {
				// DO NOTHING
			}
		});
	}
	
	@Test
	public void testUnion2() {
		final int NUM_INPUTS = 4;
		
		// construct the plan it will be multiple flat maps, all unioned
		// and the "unioned" inputDataSet will be grouped
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> source = env.readTextFile(IN_FILE);
		DataSet<Tuple2<String, Integer>> lastUnion = source.flatMap(new DummyFlatMap());
	
		for (int i = 1; i< NUM_INPUTS; i++){
			lastUnion = lastUnion.union(source.flatMap(new DummyFlatMap()));
		}
		
		DataSet<Tuple2<String, Integer>> result = lastUnion.groupBy(0).aggregate(Aggregations.SUM, 1);
		result.writeAsText(OUT_FILE);
	
		// return the plan
		Plan plan = env.createProgramPlan("Test union on new java-api");
		OptimizedPlan oPlan = compileNoStats(plan);
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		// Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				
				/* Test on the union output connections
				 * It must be under the GroupOperator and the strategy should be forward
				 */
				if (visitable instanceof SingleInputPlanNode && visitable.getProgramOperator() instanceof GroupReduceOperatorBase){
					final Channel inConn = ((SingleInputPlanNode) visitable).getInput();
					Assert.assertTrue("Union should just forward the Partitioning",
							inConn.getShipStrategy() == ShipStrategyType.FORWARD ); 
					Assert.assertTrue("Union Node should be under Group operator",
							inConn.getSource() instanceof NAryUnionPlanNode );
				}
				
				/* Test on the union input connections
				 * Must be NUM_INPUTS input connections, all FlatMapOperators with a own partitioning strategy (probably hash)
				 */
				if (visitable instanceof NAryUnionPlanNode) {
					int numberInputs = 0;
					for (Iterator<Channel> inputs = visitable.getInputs().iterator(); inputs.hasNext(); numberInputs++) {
						final Channel inConn = inputs.next();
						PlanNode inNode = inConn.getSource();
						Assert.assertTrue("Input of Union should be FlatMapOperators",
								inNode.getProgramOperator() instanceof FlatMapOperatorBase);
						Assert.assertTrue("Shipment strategy under union should partition the data",
								inConn.getShipStrategy() == ShipStrategyType.PARTITION_HASH); 
					}
					
					Assert.assertTrue("NAryUnion should have " + NUM_INPUTS + " inputs", numberInputs == NUM_INPUTS);
					return false;
				}
				return true;
			}
			
			@Override
			public void postVisit(PlanNode visitable) {
				// DO NOTHING
			}
		});
	}

	public static final class DummyFlatMap extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			out.collect(new Tuple2<String, Integer>(value, 0));
		}
	}
}
