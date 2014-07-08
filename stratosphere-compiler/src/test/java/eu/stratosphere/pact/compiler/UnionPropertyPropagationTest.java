/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.base.FlatMapOperatorBase;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.NAryUnionPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Visitor;


@SuppressWarnings("serial")
public class UnionPropertyPropagationTest extends CompilerTestBase {

	@SuppressWarnings({ "deprecation", "unchecked" })
	@Test
	public void testUnionPropertyOldApiPropagation() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(new DummyInputFormat(), IN_FILE);
		FileDataSource sourceB = new FileDataSource(new DummyInputFormat(), IN_FILE);
		
		ReduceOperator redA = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
			.input(sourceA)
			.build();
		ReduceOperator redB = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
			.input(sourceB)
			.build();
		
		ReduceOperator globalRed = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0).build();
		globalRed.addInput(redA);
		globalRed.addInput(redB);
		
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, globalRed);
		
		// return the plan
		Plan plan = new Plan(sink, "Union Property Propagation");
		
		OptimizedPlan oPlan = compileNoStats(plan);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		// Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof ReduceOperator) {
					for (Iterator<Channel> inputs = visitable.getInputs(); inputs.hasNext();) {
						final Channel inConn = inputs.next();
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
	public void testUnionNewApiAssembly() {
		final int NUM_INPUTS = 4;
		
		// construct the plan it will be multiple flat maps, all unioned
		// and the "unioned" dataSet will be grouped
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
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		// Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				
				/* Test on the union output connections
				 * It must be under the GroupOperator and the strategy should be forward
				 */
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof GroupReduceOperatorBase){
					final Channel inConn = ((SingleInputPlanNode) visitable).getInput();
					Assert.assertTrue("Union should just forward the Partitioning",
							inConn.getShipStrategy() == ShipStrategyType.FORWARD ); 
					Assert.assertTrue("Union Node should be under Group operator",
							inConn.getSource() instanceof NAryUnionPlanNode );
				}
				
				/* Test on the union input connections
				 * Must be NUM_INPUTS input connections, all FlatMapOperators with a own partitioning strategy(propably hash)
				 */
				if (visitable instanceof NAryUnionPlanNode) {
					int numberInputs = 0;
					for (Iterator<Channel> inputs = visitable.getInputs(); inputs.hasNext(); numberInputs++) {
						final Channel inConn = inputs.next();
						PlanNode inNode = inConn.getSource();
						Assert.assertTrue("Input of Union should be FlatMapOperators",
								inNode.getPactContract() instanceof FlatMapOperatorBase);
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

	public static final class DummyFlatMap extends FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			out.collect(new Tuple2<String, Integer>(value, 0));
		}
	}
}
