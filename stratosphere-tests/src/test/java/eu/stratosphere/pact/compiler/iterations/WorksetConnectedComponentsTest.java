/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.compiler.iterations;

import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.functions.StubAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.operators.WorksetIteration;
import eu.stratosphere.api.operators.util.FieldList;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.api.record.functions.MatchStub;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.compiler.dag.TempMode;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.example.record.connectedcomponents.DuplicateLongInputFormat;
import eu.stratosphere.example.record.connectedcomponents.LongLongInputFormat;
import eu.stratosphere.example.record.connectedcomponents.WorksetConnectedComponents;
import eu.stratosphere.example.record.connectedcomponents.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.example.record.connectedcomponents.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.pact.compiler.CompilerTestBase;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.util.Collector;

/**
 *
 */
public class WorksetConnectedComponentsTest extends CompilerTestBase {
	
	private static final String VERTEX_SOURCE = "Vertices";
	
	private static final String ITERATION_NAME = "Connected Components Iteration";
	
	private static final String EDGES_SOURCE = "Edges";
	private static final String JOIN_NEIGHBORS_MATCH = "Join Candidate Id With Neighbor";
	private static final String MIN_ID_REDUCER = "Find Minimum Candidate Id";
	private static final String UPDATE_ID_MATCH = "Update Component Id";
	
	private static final String SINK = "Result";
	
	private static final boolean PRINT_PLAN = false;
	
	private final FieldList set0 = new FieldList(0);
	
	
	@Test
	public void testWorksetConnectedComponents() {
		WorksetConnectedComponents cc = new WorksetConnectedComponents();

		Plan plan = cc.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE, IN_FILE, OUT_FILE, String.valueOf(100));

		OptimizedPlan optPlan = compileNoStats(plan);
		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);
		
		if (PRINT_PLAN) {
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			String json = dumper.getOptimizerPlanAsJSON(optPlan);
			System.out.println(json);
		}
		
		SourcePlanNode vertexSource = or.getNode(VERTEX_SOURCE);
		SourcePlanNode edgesSource = or.getNode(EDGES_SOURCE);
		SinkPlanNode sink = or.getNode(SINK);
		WorksetIterationPlanNode iter = or.getNode(ITERATION_NAME);
		
		DualInputPlanNode neighborsJoin = or.getNode(JOIN_NEIGHBORS_MATCH);
		SingleInputPlanNode minIdReducer = or.getNode(MIN_ID_REDUCER);
		SingleInputPlanNode minIdCombiner = (SingleInputPlanNode) minIdReducer.getPredecessor(); 
		DualInputPlanNode updatingMatch = or.getNode(UPDATE_ID_MATCH);
		
		// test all drivers
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, vertexSource.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, edgesSource.getDriverStrategy());
		
//		Assert.assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, neighborsJoin.getDriverStrategy());
		Assert.assertEquals(set0, neighborsJoin.getKeysForInput1());
		Assert.assertEquals(set0, neighborsJoin.getKeysForInput2());
		
		Assert.assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, updatingMatch.getDriverStrategy());
		Assert.assertEquals(set0, updatingMatch.getKeysForInput1());
		Assert.assertEquals(set0, updatingMatch.getKeysForInput2());
		
		// test all the shipping strategies
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iter.getInitialSolutionSetInput().getShipStrategy());
		Assert.assertEquals(set0, iter.getInitialSolutionSetInput().getShipStrategyKeys());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iter.getInitialWorksetInput().getShipStrategy());
		Assert.assertEquals(set0, iter.getInitialWorksetInput().getShipStrategyKeys());
		
		Assert.assertEquals(ShipStrategyType.FORWARD, neighborsJoin.getInput1().getShipStrategy()); // workset
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, neighborsJoin.getInput2().getShipStrategy()); // edges
		Assert.assertEquals(set0, neighborsJoin.getInput2().getShipStrategyKeys());
		Assert.assertTrue(neighborsJoin.getInput2().getTempMode().isCached());
		
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, minIdReducer.getInput().getShipStrategy());
		Assert.assertEquals(set0, minIdReducer.getInput().getShipStrategyKeys());
		Assert.assertEquals(ShipStrategyType.FORWARD, minIdCombiner.getInput().getShipStrategy());
		
		Assert.assertEquals(ShipStrategyType.FORWARD, updatingMatch.getInput1().getShipStrategy()); // min id
		Assert.assertEquals(ShipStrategyType.FORWARD, updatingMatch.getInput2().getShipStrategy()); // solution set
		
		// test all the local strategies
		Assert.assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
		Assert.assertEquals(LocalStrategy.NONE, iter.getInitialSolutionSetInput().getLocalStrategy());
//		Assert.assertEquals(LocalStrategy.NONE, iter.getInitialWorksetInput().getLocalStrategy());
		
		Assert.assertEquals(LocalStrategy.NONE, neighborsJoin.getInput1().getLocalStrategy()); // workset
		Assert.assertEquals(LocalStrategy.NONE, neighborsJoin.getInput2().getLocalStrategy()); // edges
		
		Assert.assertEquals(LocalStrategy.COMBININGSORT, minIdReducer.getInput().getLocalStrategy());
		Assert.assertEquals(set0, minIdReducer.getInput().getLocalStrategyKeys());
		Assert.assertEquals(LocalStrategy.NONE, minIdCombiner.getInput().getLocalStrategy());
		
		Assert.assertEquals(LocalStrategy.NONE, updatingMatch.getInput1().getLocalStrategy()); // min id
		Assert.assertEquals(LocalStrategy.NONE, updatingMatch.getInput2().getLocalStrategy()); // solution set
		
		// check the dams
		Assert.assertTrue(TempMode.PIPELINE_BREAKER == iter.getInitialWorksetInput().getTempMode() ||
							LocalStrategy.SORT == iter.getInitialWorksetInput().getLocalStrategy());
		
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		jgg.compileJobGraph(optPlan);
	}
	
	@Test
	public void testWorksetConnectedComponentsWithSolutionSetAsFirstInput() {

		Plan plan = getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(DEFAULT_PARALLELISM,
				IN_FILE, IN_FILE, OUT_FILE, 100);

		OptimizedPlan optPlan = compileNoStats(plan);
		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);
		
		if (PRINT_PLAN) {
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			String json = dumper.getOptimizerPlanAsJSON(optPlan);
			System.out.println(json);
		}
		
		SourcePlanNode vertexSource = or.getNode(VERTEX_SOURCE);
		SourcePlanNode edgesSource = or.getNode(EDGES_SOURCE);
		SinkPlanNode sink = or.getNode(SINK);
		WorksetIterationPlanNode iter = or.getNode(ITERATION_NAME);
		
		DualInputPlanNode neighborsJoin = or.getNode(JOIN_NEIGHBORS_MATCH);
		SingleInputPlanNode minIdReducer = or.getNode(MIN_ID_REDUCER);
		SingleInputPlanNode minIdCombiner = (SingleInputPlanNode) minIdReducer.getPredecessor(); 
		DualInputPlanNode updatingMatch = or.getNode(UPDATE_ID_MATCH);
		
		// test all drivers
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, vertexSource.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, edgesSource.getDriverStrategy());
		
//		Assert.assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, neighborsJoin.getDriverStrategy());
		Assert.assertEquals(set0, neighborsJoin.getKeysForInput1());
		Assert.assertEquals(set0, neighborsJoin.getKeysForInput2());
		
		Assert.assertEquals(DriverStrategy.HYBRIDHASH_BUILD_FIRST, updatingMatch.getDriverStrategy());
		Assert.assertEquals(set0, updatingMatch.getKeysForInput1());
		Assert.assertEquals(set0, updatingMatch.getKeysForInput2());
		
		// test all the shipping strategies
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iter.getInitialSolutionSetInput().getShipStrategy());
		Assert.assertEquals(set0, iter.getInitialSolutionSetInput().getShipStrategyKeys());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iter.getInitialWorksetInput().getShipStrategy());
		Assert.assertEquals(set0, iter.getInitialWorksetInput().getShipStrategyKeys());
		
		Assert.assertEquals(ShipStrategyType.FORWARD, neighborsJoin.getInput1().getShipStrategy()); // workset
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, neighborsJoin.getInput2().getShipStrategy()); // edges
		Assert.assertEquals(set0, neighborsJoin.getInput2().getShipStrategyKeys());
		Assert.assertTrue(neighborsJoin.getInput2().getTempMode().isCached());
		
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, minIdReducer.getInput().getShipStrategy());
		Assert.assertEquals(set0, minIdReducer.getInput().getShipStrategyKeys());
		Assert.assertEquals(ShipStrategyType.FORWARD, minIdCombiner.getInput().getShipStrategy());
		
		Assert.assertEquals(ShipStrategyType.FORWARD, updatingMatch.getInput1().getShipStrategy()); // solution set
		Assert.assertEquals(ShipStrategyType.FORWARD, updatingMatch.getInput2().getShipStrategy()); // min id
		
		// test all the local strategies
		Assert.assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
		Assert.assertEquals(LocalStrategy.NONE, iter.getInitialSolutionSetInput().getLocalStrategy());
//		Assert.assertEquals(LocalStrategy.NONE, iter.getInitialWorksetInput().getLocalStrategy());
		
		Assert.assertEquals(LocalStrategy.NONE, neighborsJoin.getInput1().getLocalStrategy()); // workset
		Assert.assertEquals(LocalStrategy.NONE, neighborsJoin.getInput2().getLocalStrategy()); // edges
		
		Assert.assertEquals(LocalStrategy.COMBININGSORT, minIdReducer.getInput().getLocalStrategy());
		Assert.assertEquals(set0, minIdReducer.getInput().getLocalStrategyKeys());
		Assert.assertEquals(LocalStrategy.NONE, minIdCombiner.getInput().getLocalStrategy());
		
		Assert.assertEquals(LocalStrategy.NONE, updatingMatch.getInput1().getLocalStrategy()); // min id
		Assert.assertEquals(LocalStrategy.NONE, updatingMatch.getInput2().getLocalStrategy()); // solution set
		
		// check the dams
		Assert.assertTrue(TempMode.PIPELINE_BREAKER == iter.getInitialWorksetInput().getTempMode() ||
							LocalStrategy.SORT == iter.getInitialWorksetInput().getLocalStrategy());
		
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		jgg.compileJobGraph(optPlan);
	}
	
	
	@ConstantFieldsSecond(0)
	public static final class UpdateComponentIdMatchMirrored extends MatchStub implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void match(PactRecord currentVertexWithComponent, PactRecord newVertexWithComponent, Collector<PactRecord> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, PactLong.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, PactLong.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	private static Plan getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(
			int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations)
	{
		// create DataSourceContract for the vertices
		FileDataSource initialVertices = new FileDataSource(new DuplicateLongInputFormat(), verticesInput, "Vertices");
		
		WorksetIteration iteration = new WorksetIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(initialVertices);
		iteration.setInitialWorkset(initialVertices);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(new LongLongInputFormat(), edgeInput, "Edges");

		// create CrossOperator for distance computation
		JoinOperator joinWithNeighbors = JoinOperator.builder(new NeighborWithComponentIDJoin(), PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// create ReduceOperator for finding the nearest cluster centers
		ReduceOperator minCandidateId = ReduceOperator.builder(new MinimumComponentIDReduce(), PactLong.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// create CrossOperator for distance computation
		JoinOperator updateComponentId = JoinOperator.builder(new UpdateComponentIdMatchMirrored(), PactLong.class, 0, 0)
				.input1(iteration.getSolutionSet())
				.input2(minCandidateId)
				.name("Update Component Id")
				.build();
		
		iteration.setNextWorkset(updateComponentId);
		iteration.setSolutionSetDelta(updateComponentId);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, iteration, "Result");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactLong.class, 0)
			.field(PactLong.class, 1);

		// return the PACT plan
		Plan plan = new Plan(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
}
