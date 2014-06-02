/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.spargel.java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.spargel.java.examples.SpargelConnectedComponents.CCMessager;
import eu.stratosphere.spargel.java.examples.SpargelConnectedComponents.CCUpdater;
import eu.stratosphere.spargel.java.examples.SpargelConnectedComponents.IdAssigner;
import eu.stratosphere.test.compiler.util.CompilerTestBase;


public class SpargelCompilerTest extends CompilerTestBase {

//	@Test
	public void testSpargelCompiler() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(DEFAULT_PARALLELISM);
			// compose test program
			{
				DataSet<Long> vertexIds = env.generateSequence(1, 2);
				
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
				
				DataSet<Tuple2<Long, Long>> initialVertices = vertexIds.map(new IdAssigner());
				DataSet<Tuple2<Long, Long>> result = initialVertices.runOperation(VertexCentricIteration.withPlainEdges(edges, new CCUpdater(), new CCMessager(), 100));
				
				result.print();
			}
			
			Plan p = env.createProgramPlan("Spargel Connected Components");
			OptimizedPlan op = compileNoStats(p);
			
			// check the sink
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
			assertEquals(DEFAULT_PARALLELISM, sink.getDegreeOfParallelism());
			
			// check the iteration
			WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
			assertEquals(DEFAULT_PARALLELISM, iteration.getDegreeOfParallelism());
			
			// check the solution set join and the delta
			PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
			assertTrue(ssDelta instanceof DualInputPlanNode); // this is only true if the update functions preserves the partitioning
			
			DualInputPlanNode ssJoin = (DualInputPlanNode) ssDelta;
			assertEquals(DEFAULT_PARALLELISM, ssJoin.getDegreeOfParallelism());
			assertEquals(ShipStrategyType.PARTITION_HASH, ssJoin.getInput1().getShipStrategy());
			assertEquals(new FieldList(0), ssJoin.getInput1().getShipStrategyKeys());
			
			// check the workset set join
			DualInputPlanNode edgeJoin = (DualInputPlanNode) ssJoin.getInput1().getSource();
			assertEquals(DEFAULT_PARALLELISM, edgeJoin.getDegreeOfParallelism());
			assertEquals(ShipStrategyType.PARTITION_HASH, edgeJoin.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, edgeJoin.getInput2().getShipStrategy());
			assertTrue(edgeJoin.getInput1().getTempMode().isCached());
			
			assertEquals(new FieldList(0), edgeJoin.getInput1().getShipStrategyKeys());
			
			// check that the initial partitioning is pushed out of the loop
			assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput2().getShipStrategy());
			assertEquals(new FieldList(0), iteration.getInput1().getShipStrategyKeys());
			assertEquals(new FieldList(0), iteration.getInput2().getShipStrategyKeys());
			
			// check that the initial workset sort is outside the loop
			assertEquals(LocalStrategy.SORT, iteration.getInput2().getLocalStrategy());
			assertEquals(new FieldList(0), iteration.getInput2().getLocalStrategyKeys());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSpargelCompilerWithBroadcastVariable() {
		try {
			final String BC_VAR_NAME = "borat variable";
			
			
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(DEFAULT_PARALLELISM);
			// compose test program
			{
				DataSet<Long> bcVar = env.fromElements(1L);
				
				DataSet<Long> vertexIds = env.generateSequence(1, 2);
				
				@SuppressWarnings("unchecked")
				DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
				
				DataSet<Tuple2<Long, Long>> initialVertices = vertexIds.map(new IdAssigner());
				
				VertexCentricIteration<Long, Long, Long, ?> vcIter = VertexCentricIteration.withPlainEdges(edges, new CCUpdater(), new CCMessager(), 100);
				vcIter.addBroadcastSetForMessagingFunction(BC_VAR_NAME, bcVar);
				vcIter.addBroadcastSetForUpdateFunction(BC_VAR_NAME, bcVar);
				
				DataSet<Tuple2<Long, Long>> result = initialVertices.runOperation(vcIter);
				
				result.print();
			}
			
			Plan p = env.createProgramPlan("Spargel Connected Components");
			OptimizedPlan op = compileNoStats(p);
			
			// check the sink
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
			assertEquals(DEFAULT_PARALLELISM, sink.getDegreeOfParallelism());
			
			// check the iteration
			WorksetIterationPlanNode iteration = (WorksetIterationPlanNode) sink.getInput().getSource();
			assertEquals(DEFAULT_PARALLELISM, iteration.getDegreeOfParallelism());
			
			// check the solution set join and the delta
			PlanNode ssDelta = iteration.getSolutionSetDeltaPlanNode();
			assertTrue(ssDelta instanceof DualInputPlanNode); // this is only true if the update functions preserves the partitioning
			
			DualInputPlanNode ssJoin = (DualInputPlanNode) ssDelta;
			assertEquals(DEFAULT_PARALLELISM, ssJoin.getDegreeOfParallelism());
			assertEquals(ShipStrategyType.PARTITION_HASH, ssJoin.getInput1().getShipStrategy());
			assertEquals(new FieldList(0), ssJoin.getInput1().getShipStrategyKeys());
			
			// check the workset set join
			DualInputPlanNode edgeJoin = (DualInputPlanNode) ssJoin.getInput1().getSource();
			assertEquals(DEFAULT_PARALLELISM, edgeJoin.getDegreeOfParallelism());
			assertEquals(ShipStrategyType.PARTITION_HASH, edgeJoin.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, edgeJoin.getInput2().getShipStrategy());
			assertTrue(edgeJoin.getInput1().getTempMode().isCached());
			
			assertEquals(new FieldList(0), edgeJoin.getInput1().getShipStrategyKeys());
			
			// check that the initial partitioning is pushed out of the loop
			assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, iteration.getInput2().getShipStrategy());
			assertEquals(new FieldList(0), iteration.getInput1().getShipStrategyKeys());
			assertEquals(new FieldList(0), iteration.getInput2().getShipStrategyKeys());
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
