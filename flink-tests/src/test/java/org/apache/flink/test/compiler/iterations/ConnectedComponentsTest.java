/**
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

package org.apache.flink.test.compiler.iterations;

import java.io.Serializable;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.DeltaIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.compiler.dag.TempMode;
import org.apache.flink.compiler.plan.DualInputPlanNode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.compiler.plan.SourcePlanNode;
import org.apache.flink.compiler.plan.WorksetIterationPlanNode;
import org.apache.flink.compiler.plandump.PlanJSONDumpGenerator;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.test.compiler.util.CompilerTestBase;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.DuplicateLongMap;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.MinimumComponentIDReduce;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ConnectedComponentsTest extends CompilerTestBase {
	
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
		
		Assert.assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED, neighborsJoin.getDriverStrategy());
		Assert.assertTrue(!neighborsJoin.getInput1().getTempMode().isCached());
		Assert.assertTrue(!neighborsJoin.getInput2().getTempMode().isCached());
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
		
		Assert.assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED, neighborsJoin.getDriverStrategy());
		Assert.assertTrue(!neighborsJoin.getInput1().getTempMode().isCached());
		Assert.assertTrue(!neighborsJoin.getInput2().getTempMode().isCached());
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
	public static final class UpdateComponentIdMatchMirrored extends JoinFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record currentVertexWithComponent, Record newVertexWithComponent, Collector<Record> out){
	
			long candidateComponentID = newVertexWithComponent.getField(1, LongValue.class).getValue();
			long currentComponentID = currentVertexWithComponent.getField(1, LongValue.class).getValue();
	
			if (candidateComponentID < currentComponentID) {
				out.collect(newVertexWithComponent);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Plan getPlanForWorksetConnectedComponentsWithSolutionSetAsFirstInput(
			int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations)
	{
		// create DataSourceContract for the vertices
		FileDataSource initialVertices = new FileDataSource(new CsvInputFormat(' ', LongValue.class), verticesInput, "Vertices");
		
		MapOperator verticesWithId = MapOperator.builder(DuplicateLongMap.class).input(initialVertices).name("Assign Vertex Ids").build();
		
		DeltaIteration iteration = new DeltaIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(verticesWithId);
		iteration.setInitialWorkset(verticesWithId);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(new CsvInputFormat(' ', LongValue.class, LongValue.class), edgeInput, "Edges");

		// create CrossOperator for distance computation
		JoinOperator joinWithNeighbors = JoinOperator.builder(new NeighborWithComponentIDJoin(), LongValue.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		// create ReduceOperator for finding the nearest cluster centers
		ReduceOperator minCandidateId = ReduceOperator.builder(new MinimumComponentIDReduce(), LongValue.class, 0)
				.input(joinWithNeighbors)
				.name("Find Minimum Candidate Id")
				.build();
		
		// create CrossOperator for distance computation
		JoinOperator updateComponentId = JoinOperator.builder(new UpdateComponentIdMatchMirrored(), LongValue.class, 0, 0)
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
			.field(LongValue.class, 0)
			.field(LongValue.class, 1);

		// return the plan
		Plan plan = new Plan(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
}
