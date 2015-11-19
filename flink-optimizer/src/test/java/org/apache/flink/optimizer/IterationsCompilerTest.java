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

import org.apache.flink.api.common.functions.*;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.junit.Test;

import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityKeyExtractor;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Collector;

import java.util.Iterator;

@SuppressWarnings({"serial", "unchecked"})
public class IterationsCompilerTest extends CompilerTestBase {

	@Test
	public void testSolutionSetDeltaDependsOnBroadcastVariable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> source =
						env.generateSequence(1, 1000).map(new DuplicateValueScalar<Long>());
			
			DataSet<Tuple2<Long, Long>> invariantInput =
					env.generateSequence(1, 1000).map(new DuplicateValueScalar<Long>());
			
			// iteration from here
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iter = source.iterateDelta(source, 1000, 1);
			
			DataSet<Tuple2<Long, Long>> result =
				invariantInput
					.map(new IdentityMapper<Tuple2<Long, Long>>()).withBroadcastSet(iter.getWorkset(), "bc data")
					.join(iter.getSolutionSet()).where(0).equalTo(1).projectFirst(1).projectSecond(1);
			
			iter.closeWith(result.map(new IdentityMapper<Tuple2<Long,Long>>()), result)
					.output(new DiscardingOutputFormat<Tuple2<Long,Long>>());
			
			OptimizedPlan p = compileNoStats(env.createProgramPlan());
			
			// check that the JSON generator accepts this plan
			new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(p);
			
			// check that the JobGraphGenerator accepts the plan
			new JobGraphGenerator().compileJobGraph(p);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoIterationsWithMapperInbetween() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);
			
			DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> bulkResult = doBulkIteration(verticesWithInitialId, edges);
			
			DataSet<Tuple2<Long, Long>> mappedBulk = bulkResult.map(new DummyMap());
			
			DataSet<Tuple2<Long, Long>> depResult = doDeltaIteration(mappedBulk, edges);
			
			depResult.output(new DiscardingOutputFormat<Tuple2<Long,Long>>());
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof WorksetIterationPlanNode);
			
			WorksetIterationPlanNode wipn = (WorksetIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			assertEquals(ShipStrategyType.PARTITION_HASH, wipn.getInput1().getShipStrategy());
			
			assertEquals(TempMode.NONE, wipn.getInput1().getTempMode());
			assertEquals(TempMode.NONE, wipn.getInput2().getTempMode());

			assertEquals(DataExchangeMode.BATCH, wipn.getInput1().getDataExchangeMode());
			assertEquals(DataExchangeMode.BATCH, wipn.getInput2().getDataExchangeMode());
			
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoIterationsDirectlyChained() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);
			
			DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> bulkResult = doBulkIteration(verticesWithInitialId, edges);
			
			DataSet<Tuple2<Long, Long>> depResult = doDeltaIteration(bulkResult, edges);
			
			depResult.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof WorksetIterationPlanNode);
			
			WorksetIterationPlanNode wipn = (WorksetIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			BulkIterationPlanNode bipn = (BulkIterationPlanNode)wipn.getInput1().getSource();

			// the hash partitioning has been pushed out of the delta iteration into the bulk iteration
			assertEquals(ShipStrategyType.FORWARD, wipn.getInput1().getShipStrategy());

			// the input of the root step function is the last operator of the step function
			// since the work has been pushed out of the bulk iteration, it has to guarantee the hash partitioning
			for (Channel c : bipn.getRootOfStepFunction().getInputs()) {
				assertEquals(ShipStrategyType.PARTITION_HASH, c.getShipStrategy());
			}

			assertEquals(DataExchangeMode.BATCH, wipn.getInput1().getDataExchangeMode());
			assertEquals(DataExchangeMode.BATCH, wipn.getInput2().getDataExchangeMode());
			
			assertEquals(TempMode.NONE, wipn.getInput1().getTempMode());
			assertEquals(TempMode.NONE, wipn.getInput2().getTempMode());
			
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testTwoWorksetIterationsDirectlyChained() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);
			
			DataSet<Tuple2<Long, Long>> verticesWithInitialId = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> edges = env.fromElements(new Tuple2<Long, Long>(1L, 2L));
			
			DataSet<Tuple2<Long, Long>> firstResult = doDeltaIteration(verticesWithInitialId, edges);
			
			DataSet<Tuple2<Long, Long>> secondResult = doDeltaIteration(firstResult, edges);
			
			secondResult.output(new DiscardingOutputFormat<Tuple2<Long,Long>>());
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof WorksetIterationPlanNode);
			
			WorksetIterationPlanNode wipn = (WorksetIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			assertEquals(ShipStrategyType.FORWARD, wipn.getInput1().getShipStrategy());

			assertEquals(DataExchangeMode.BATCH, wipn.getInput1().getDataExchangeMode());
			assertEquals(DataExchangeMode.BATCH, wipn.getInput2().getDataExchangeMode());

			assertEquals(TempMode.NONE, wipn.getInput1().getTempMode());
			assertEquals(TempMode.NONE, wipn.getInput2().getTempMode());
			
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testIterationPushingWorkOut() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);
			
			DataSet<Tuple2<Long, Long>> input1 = env.readCsvFile("/some/file/path").types(Long.class).map(new DuplicateValue());
			
			DataSet<Tuple2<Long, Long>> input2 = env.readCsvFile("/some/file/path").types(Long.class, Long.class);

			// we do two join operations with input1 which is the partial solution
			// it is cheaper to push the partitioning out so that the feedback channel and the
			// initial input do the partitioning
			doBulkIteration(input1, input2).output(new DiscardingOutputFormat<Tuple2<Long,Long>>());
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof BulkIterationPlanNode);
			
			BulkIterationPlanNode bipn = (BulkIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();
			
			// check that work has been pushed out
			for (Channel c : bipn.getPartialSolutionPlanNode().getOutgoingChannels()) {
				assertEquals(ShipStrategyType.FORWARD, c.getShipStrategy());
			}

			// the end of the step function has to produce the necessary properties
			for (Channel c : bipn.getRootOfStepFunction().getInputs()) {
				assertEquals(ShipStrategyType.PARTITION_HASH, c.getShipStrategy());
			}

			assertEquals(ShipStrategyType.PARTITION_HASH, bipn.getInput().getShipStrategy());
			
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testIterationNotPushingWorkOut() throws Exception {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);

			DataSet<Tuple2<Long, Long>> input1 = env.readCsvFile("/some/file/path").types(Long.class).map(new DuplicateValue());

			DataSet<Tuple2<Long, Long>> input2 = env.readCsvFile("/some/file/path").types(Long.class, Long.class);

			// Use input1 as partial solution. Partial solution is used in a single join operation --> it is cheaper
			// to do the hash partitioning between the partial solution node and the join node
			// instead of pushing the partitioning out
			doSimpleBulkIteration(input1, input2).output(new DiscardingOutputFormat<Tuple2<Long,Long>>());

			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			assertEquals(1, op.getDataSinks().size());
			assertTrue(op.getDataSinks().iterator().next().getInput().getSource() instanceof BulkIterationPlanNode);

			BulkIterationPlanNode bipn = (BulkIterationPlanNode) op.getDataSinks().iterator().next().getInput().getSource();

			// check that work has not been pushed out
			for (Channel c : bipn.getPartialSolutionPlanNode().getOutgoingChannels()) {
				assertEquals(ShipStrategyType.PARTITION_HASH, c.getShipStrategy());
			}

			assertEquals(ShipStrategyType.FORWARD, bipn.getInput().getShipStrategy());

			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWorksetIterationPipelineBreakerPlacement() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(8);
			
			// the workset (input two of the delta iteration) is the same as what is consumed be the successive join
			DataSet<Tuple2<Long, Long>> initialWorkset = env.readCsvFile("/some/file/path").types(Long.class).map(new DuplicateValue());
			
			DataSet<Tuple2<Long, Long>> initialSolutionSet = env.readCsvFile("/some/file/path").types(Long.class).map(new DuplicateValue());
			
			// trivial iteration, since we are interested in the inputs to the iteration
			DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = initialSolutionSet.iterateDelta(initialWorkset, 100, 0);
			
			DataSet<Tuple2<Long, Long>> next = iteration.getWorkset().map(new IdentityMapper<Tuple2<Long,Long>>());
			
			DataSet<Tuple2<Long, Long>> result = iteration.closeWith(next, next);
			
			initialWorkset
				.join(result, JoinHint.REPARTITION_HASH_FIRST)
				.where(0).equalTo(0)
				.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());
			
			Plan p = env.createProgramPlan();
			compileNoStats(p);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testResetPartialSolution() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Long> width = env.generateSequence(1, 10);
			DataSet<Long> update = env.generateSequence(1, 10);
			DataSet<Long> lastGradient = env.generateSequence(1, 10);
			
			DataSet<Long> init = width.union(update).union(lastGradient);
			
			IterativeDataSet<Long> iteration = init.iterate(10);
			
			width = iteration.filter(new IdFilter<Long>());
			update = iteration.filter(new IdFilter<Long>());
			lastGradient = iteration.filter(new IdFilter<Long>());
			
			DataSet<Long> gradient = width.map(new IdentityMapper<Long>());
			DataSet<Long> term = gradient.join(lastGradient)
								.where(new IdentityKeyExtractor<Long>())
								.equalTo(new IdentityKeyExtractor<Long>())
								.with(new JoinFunction<Long, Long, Long>() {
									public Long join(Long first, Long second) { return null; }
								});
			
			update = update.map(new RichMapFunction<Long, Long>() {
				public Long map(Long value) { return null; }
			}).withBroadcastSet(term, "some-name");
			
			DataSet<Long> result = iteration.closeWith(width.union(update).union(lastGradient));
			
			result.output(new DiscardingOutputFormat<Long>());
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			new JobGraphGenerator().compileJobGraph(op);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that interesting properties can be pushed out of the bulk iteration. This requires
	 * that a NoOp node is appended to the step function which re-establishes the properties of
	 * the initial input. If this does not work, then Flink won't find a plan, because the optimizer
	 * will not consider plans where the partitioning is done after the partial solution node in
	 * this case (because of pruning).
	 * @throws Exception
	 */
	@Test
	public void testBulkIterationWithPartialSolutionProperties() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple1<Long>> input1 = env.generateSequence(1, 10).map(new MapFunction<Long, Tuple1<Long>>() {
			@Override
			public Tuple1<Long> map(Long value) throws Exception {
				return new Tuple1<>(value);
			}
		});

		DataSet<Tuple1<Long>> input2 = env.generateSequence(1, 10).map(new MapFunction<Long, Tuple1<Long>>() {
			@Override
			public Tuple1<Long> map(Long value) throws Exception {
				return new Tuple1<>(value);
			}
		});

		DataSet<Tuple1<Long>> distinctInput = input1.distinct();

		IterativeDataSet<Tuple1<Long>> iteration = distinctInput.iterate(10);

		DataSet<Tuple1<Long>> iterationStep = iteration
				.coGroup(input2)
				.where(0)
				.equalTo(0)
				.with(new CoGroupFunction<Tuple1<Long>, Tuple1<Long>, Tuple1<Long>>() {
					@Override
					public void coGroup(
							Iterable<Tuple1<Long>> first,
							Iterable<Tuple1<Long>> second,
							Collector<Tuple1<Long>> out) throws Exception {
						Iterator<Tuple1<Long>> it = first.iterator();

						if (it.hasNext()) {
							out.collect(it.next());
						}
					}
				});

		DataSet<Tuple1<Long>> iterationResult = iteration.closeWith(iterationStep);

		iterationResult.output(new DiscardingOutputFormat<Tuple1<Long>>());

		Plan p = env.createProgramPlan();
		OptimizedPlan op = compileNoStats(p);

		new JobGraphGenerator().compileJobGraph(op);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static DataSet<Tuple2<Long, Long>> doBulkIteration(DataSet<Tuple2<Long, Long>> vertices, DataSet<Tuple2<Long, Long>> edges) {
		
		// open a bulk iteration
		IterativeDataSet<Tuple2<Long, Long>> iteration = vertices.iterate(20);
		
		DataSet<Tuple2<Long, Long>> changes = iteration
				.join(edges).where(0).equalTo(0).with(new Join222())
				.groupBy(0).aggregate(Aggregations.MIN, 1)
				.join(iteration).where(0).equalTo(0)
				.flatMap(new FlatMapJoin());
		
		// close the bulk iteration
		return iteration.closeWith(changes);
	}

	public static DataSet<Tuple2<Long, Long>> doSimpleBulkIteration(DataSet<Tuple2<Long, Long>> vertices, DataSet<Tuple2<Long, Long>> edges) {

		// open a bulk iteration
		IterativeDataSet<Tuple2<Long, Long>> iteration = vertices.iterate(20);

		DataSet<Tuple2<Long, Long>> changes = iteration
				.join(edges).where(0).equalTo(0)
				.flatMap(new FlatMapJoin());

		// close the bulk iteration
		return iteration.closeWith(changes);
	}
				
		
	public static DataSet<Tuple2<Long, Long>> doDeltaIteration(DataSet<Tuple2<Long, Long>> vertices, DataSet<Tuple2<Long, Long>> edges) {

		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> depIteration = vertices.iterateDelta(vertices, 100, 0);
				
		DataSet<Tuple1<Long>> candidates = depIteration.getWorkset().join(edges).where(0).equalTo(0)
				.projectSecond(1);
		
		DataSet<Tuple1<Long>> grouped = candidates.groupBy(0).reduceGroup(new Reduce101());
		
		DataSet<Tuple2<Long, Long>> candidatesDependencies = 
				grouped.join(edges).where(0).equalTo(1).projectSecond(0, 1);
		
		DataSet<Tuple2<Long, Long>> verticesWithNewComponents = 
				candidatesDependencies.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new Join222())
				.groupBy(0).aggregate(Aggregations.MIN, 1);
		
		DataSet<Tuple2<Long, Long>> updatedComponentId = 
				verticesWithNewComponents.join(depIteration.getSolutionSet()).where(0).equalTo(0)
				.flatMap(new FlatMapJoin());
		
		DataSet<Tuple2<Long, Long>> depResult = depIteration.closeWith(updatedComponentId, updatedComponentId);

		return depResult;
		
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class Join222 extends RichJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return null;
		}
	}
	
	public static final class FlatMapJoin extends RichFlatMapFunction<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {
		
		@Override
		public void flatMap(Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> value, Collector<Tuple2<Long, Long>> out) {}
	}
	
	public static final class DummyMap extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
			return value;
		}
	}
	
	@ForwardedFields("0")
	public static final class Reduce101 extends RichGroupReduceFunction<Tuple1<Long>, Tuple1<Long>> {
		
		@Override
		public void reduce(Iterable<Tuple1<Long>> values, Collector<Tuple1<Long>> out) {}
	}
	
	@ForwardedFields("0")
	public static final class DuplicateValue extends RichMapFunction<Tuple1<Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> map(Tuple1<Long> value) throws Exception {
			return new Tuple2<Long, Long>(value.f0, value.f0);
		}
	}
	
	public static final class DuplicateValueScalar<T> extends RichMapFunction<T, Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}
	
	public static final class IdFilter<T> implements FilterFunction<T> {
		@Override
		public boolean filter(T value) {
			return true;
		}
	}
}
