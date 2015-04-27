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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.testfunctions.DummyCoGroupFunction;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.record.operators.BulkIteration;
import org.apache.flink.api.java.record.operators.CoGroupOperator;
import org.apache.flink.api.java.record.operators.CrossOperator;
import org.apache.flink.api.java.record.operators.DeltaIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.testfunctions.IdentityKeyExtractor;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.testfunctions.Top1GroupReducer;
import org.apache.flink.optimizer.util.DummyCoGroupStub;
import org.apache.flink.optimizer.util.DummyCrossStub;
import org.apache.flink.optimizer.util.DummyInputFormat;
import org.apache.flink.optimizer.util.DummyMatchStub;
import org.apache.flink.optimizer.util.DummyNonPreservingMatchStub;
import org.apache.flink.optimizer.util.DummyOutputFormat;
import org.apache.flink.optimizer.util.IdentityMap;
import org.apache.flink.optimizer.util.IdentityReduce;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

@SuppressWarnings({"serial", "deprecation"})
public class BranchingPlansCompilerTest extends CompilerTestBase {
	
	
	@Test
	public void testCostComputationWithMultipleDataSinks() {
		final int SINKS = 5;
	
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(DEFAULT_PARALLELISM);

			DataSet<Long> source = env.generateSequence(1, 10000);

			DataSet<Long> mappedA = source.map(new IdentityMapper<Long>());
			DataSet<Long> mappedC = source.map(new IdentityMapper<Long>());

			for (int sink = 0; sink < SINKS; sink++) {
				mappedA.output(new DiscardingOutputFormat<Long>());
				mappedC.output(new DiscardingOutputFormat<Long>());
			}

			Plan plan = env.createProgramPlan("Plans With Multiple Data Sinks");
			OptimizedPlan oPlan = compileNoStats(plan);

			new JobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	/**
	 * 
	 * <pre>
	 *                (SRC A)  
	 *                   |
	 *                (MAP A)
	 *             /         \   
	 *          (MAP B)      (MAP C)
	 *           /           /     \
	 *        (SINK A)    (SINK B)  (SINK C)
	 * </pre>
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testBranchingWithMultipleDataSinks2() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(DEFAULT_PARALLELISM);

			DataSet<Long> source = env.generateSequence(1, 10000);

			DataSet<Long> mappedA = source.map(new IdentityMapper<Long>());
			DataSet<Long> mappedB = mappedA.map(new IdentityMapper<Long>());
			DataSet<Long> mappedC = mappedA.map(new IdentityMapper<Long>());

			mappedB.output(new DiscardingOutputFormat<Long>());
			mappedC.output(new DiscardingOutputFormat<Long>());
			mappedC.output(new DiscardingOutputFormat<Long>());

			Plan plan = env.createProgramPlan();
			Set<Operator<?>> sinks = new HashSet<Operator<?>>(plan.getDataSinks());

			OptimizedPlan oPlan = compileNoStats(plan);

			// ---------- check the optimizer plan ----------

			// number of sinks
			assertEquals("Wrong number of data sinks.", 3, oPlan.getDataSinks().size());

			// remove matching sinks to check relation
			for (SinkPlanNode sink : oPlan.getDataSinks()) {
				assertTrue(sinks.remove(sink.getProgramOperator()));
			}
			assertTrue(sinks.isEmpty());

			new JobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	

	/**
	 * <pre>
	 *                              SINK
	 *                               |
	 *                            COGROUP
	 *                        +---/    \----+
	 *                       /               \
	 *                      /             MATCH10
	 *                     /               |    \
	 *                    /                |  MATCH9
	 *                MATCH5               |  |   \
	 *                |   \                |  | MATCH8
	 *                | MATCH4             |  |  |   \
	 *                |  |   \             |  |  | MATCH7
	 *                |  | MATCH3          |  |  |  |   \
	 *                |  |  |   \          |  |  |  | MATCH6
	 *                |  |  | MATCH2       |  |  |  |  |  |
	 *                |  |  |  |   \       +--+--+--+--+--+
	 *                |  |  |  | MATCH1            MAP 
	 *                \  |  |  |  |  | /-----------/
	 *                (DATA SOURCE ONE)
	 * </pre>
	 */
	@Test
	public void testBranchingSourceMultipleTimes() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(DEFAULT_PARALLELISM);

			DataSet<Tuple2<Long, Long>> source = env.generateSequence(1, 10000000)
				.map(new Duplicator<Long>());

			DataSet<Tuple2<Long, Long>> joined1 = source.join(source).where(0).equalTo(0)
														.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined2 = source.join(joined1).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined3 = source.join(joined2).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined4 = source.join(joined3).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined5 = source.join(joined4).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> mapped = source.map(
					new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> map(Tuple2<Long, Long> value) {
							return null;
						}
			});

			DataSet<Tuple2<Long, Long>> joined6 = mapped.join(mapped).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined7 = mapped.join(joined6).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined8 = mapped.join(joined7).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined9 = mapped.join(joined8).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined10 = mapped.join(joined9).where(0).equalTo(0)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());


			joined5.coGroup(joined10)
					.where(1).equalTo(1)
					.with(new DummyCoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>())

				.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());

			Plan plan = env.createProgramPlan();
			OptimizedPlan oPlan = compileNoStats(plan);
			new JobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * 
	 * <pre>

	 *              (SINK A)
	 *                  |    (SINK B)    (SINK C)
	 *                CROSS    /          /
	 *               /     \   |  +------+
	 *              /       \  | /
	 *          REDUCE      MATCH2
	 *             |    +---/    \
	 *              \  /          |
	 *               MAP          |
	 *                |           |
	 *             COGROUP      MATCH1
	 *             /     \     /     \
	 *        (SRC A)    (SRC B)    (SRC C)
	 * </pre>
	 */
	@Test
	public void testBranchingWithMultipleDataSinks() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(DEFAULT_PARALLELISM);

			DataSet<Tuple2<Long, Long>> sourceA = env.generateSequence(1, 10000000)
					.map(new Duplicator<Long>());

			DataSet<Tuple2<Long, Long>> sourceB = env.generateSequence(1, 10000000)
					.map(new Duplicator<Long>());

			DataSet<Tuple2<Long, Long>> sourceC = env.generateSequence(1, 10000000)
					.map(new Duplicator<Long>());

			DataSet<Tuple2<Long, Long>> mapped = sourceA.coGroup(sourceB)
					.where(0).equalTo(1)
					.with(new CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
							@Override
							public void coGroup(Iterable<Tuple2<Long, Long>> first,
													Iterable<Tuple2<Long, Long>> second,
													Collector<Tuple2<Long, Long>> out) {
							  }
					})
					.map(new IdentityMapper<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined = sourceB.join(sourceC)
					.where(0).equalTo(1)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> joined2 = mapped.join(joined)
					.where(1).equalTo(1)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			DataSet<Tuple2<Long, Long>> reduced = mapped
					.groupBy(1)
					.reduceGroup(new Top1GroupReducer<Tuple2<Long, Long>>());

			reduced.cross(joined2)
					.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());

			joined2.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			joined2.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

			Plan plan = env.createProgramPlan();
			OptimizedPlan oPlan = compileNoStats(plan);
			new JobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testBranchEachContractType() {
		try {
			// construct the plan
			FileDataSource sourceA = new FileDataSource(new DummyInputFormat(), "file:///test/file1", "Source A");
			FileDataSource sourceB = new FileDataSource(new DummyInputFormat(), "file:///test/file2", "Source B");
			FileDataSource sourceC = new FileDataSource(new DummyInputFormat(), "file:///test/file3", "Source C");
			
			MapOperator map1 = MapOperator.builder(new IdentityMap()).input(sourceA).name("Map 1").build();
			
			ReduceOperator reduce1 = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
				.input(map1)
				.name("Reduce 1")
				.build();
			
			JoinOperator match1 = JoinOperator.builder(new DummyMatchStub(), IntValue.class, 0, 0)
				.input1(sourceB, sourceB, sourceC)
				.input2(sourceC)
				.name("Match 1")
				.build();
			;
			CoGroupOperator cogroup1 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(sourceA)
				.input2(sourceB)
				.name("CoGroup 1")
				.build();
			
			CrossOperator cross1 = CrossOperator.builder(new DummyCrossStub())
				.input1(reduce1)
				.input2(cogroup1)
				.name("Cross 1")
				.build();
			
			
			CoGroupOperator cogroup2 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(cross1)
				.input2(cross1)
				.name("CoGroup 2")
				.build();
			
			CoGroupOperator cogroup3 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(map1)
				.input2(match1)
				.name("CoGroup 3")
				.build();
			
			
			MapOperator map2 = MapOperator.builder(new IdentityMap()).input(cogroup3).name("Map 2").build();
			
			CoGroupOperator cogroup4 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(map2)
				.input2(match1)
				.name("CoGroup 4")
				.build();
			
			CoGroupOperator cogroup5 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(cogroup2)
				.input2(cogroup1)
				.name("CoGroup 5")
				.build();
			
			CoGroupOperator cogroup6 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(reduce1)
				.input2(cogroup4)
				.name("CoGroup 6")
				.build();
			
			CoGroupOperator cogroup7 = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 0,0)
				.input1(cogroup5)
				.input2(cogroup6)
				.name("CoGroup 7")
				.build();
			
			FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, cogroup7);
			sink.addInput(sourceA);
			sink.addInput(cogroup3);
			sink.addInput(cogroup4);
			sink.addInput(cogroup1);
			
			// return the PACT plan
			Plan plan = new Plan(sink, "Branching of each contract type");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			JobGraphGenerator jobGen = new JobGraphGenerator();
			
			//Compile plan to verify that no error is thrown
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	

	@Test
	public void testBranchingUnion() {
		try {
			// construct the plan
			FileDataSource source1 = new FileDataSource(new DummyInputFormat(), IN_FILE);
			FileDataSource source2 = new FileDataSource(new DummyInputFormat(), IN_FILE);
			
			JoinOperator mat1 = JoinOperator.builder(new DummyMatchStub(), IntValue.class, 0, 0)
				.input1(source1)
				.input2(source2)
				.name("Match 1")
				.build();
			
			MapOperator ma1 = MapOperator.builder(new IdentityMap()).input(mat1).name("Map1").build();
			
			ReduceOperator r1 = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
				.input(ma1)
				.name("Reduce 1")
				.build();
			
			ReduceOperator r2 = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
				.input(mat1)
				.name("Reduce 2")
				.build();
			
			MapOperator ma2 = MapOperator.builder(new IdentityMap()).input(mat1).name("Map 2").build();
			
			MapOperator ma3 = MapOperator.builder(new IdentityMap()).input(ma2).name("Map 3").build();
			
			@SuppressWarnings("unchecked")
			JoinOperator mat2 = JoinOperator.builder(new DummyMatchStub(), IntValue.class, 0, 0)
				.input1(r1, r2, ma2, ma3)
				.input2(ma2)
				.name("Match 2")
				.build();
			mat2.setParameter(Optimizer.HINT_LOCAL_STRATEGY, Optimizer.HINT_LOCAL_STRATEGY_MERGE);
			
			FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, mat2);
			
			
			// return the PACT plan
			Plan plan = new Plan(sink, "Branching Union");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			JobGraphGenerator jobGen = new JobGraphGenerator();
			
			//Compile plan to verify that no error is thrown
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * 
	 * <pre>
	 *             (SRC A)     
	 *             /     \      
	 *        (SINK A)    (SINK B)
	 * </pre>
	 */
	@Test
	public void testBranchingWithMultipleDataSinksSmall() {
		try {
			// construct the plan
			final String out1Path = "file:///test/1";
			final String out2Path = "file:///test/2";
	
			FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
			
			FileDataSink sinkA = new FileDataSink(DummyOutputFormat.class, out1Path, sourceA);
			FileDataSink sinkB = new FileDataSink(DummyOutputFormat.class, out2Path, sourceA);
			
			List<FileDataSink> sinks = new ArrayList<FileDataSink>();
			sinks.add(sinkA);
			sinks.add(sinkB);
			
			// return the PACT plan
			Plan plan = new Plan(sinks, "Plans With Multiple Data Sinks");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			// ---------- check the optimizer plan ----------
			
			// number of sinks
			Assert.assertEquals("Wrong number of data sinks.", 2, oPlan.getDataSinks().size());
			
			// sinks contain all sink paths
			Set<String> allSinks = new HashSet<String>();
			allSinks.add(out1Path);
			allSinks.add(out2Path);
			
			for (SinkPlanNode n : oPlan.getDataSinks()) {
				String path = ((FileDataSink) n.getSinkNode().getOperator()).getFilePath();
				Assert.assertTrue("Invalid data sink.", allSinks.remove(path));
			}
			
			// ---------- compile plan to nephele job graph to verify that no error is thrown ----------
			
			JobGraphGenerator jobGen = new JobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * 
	 * <pre>
	 *     (SINK 3) (SINK 1)   (SINK 2) (SINK 4)
	 *         \     /             \     /
	 *         (SRC A)             (SRC B)
	 * </pre>
	 * 
	 * NOTE: this case is currently not caught by the compiler. we should enable the test once it is caught.
	 */
	@Test
	public void testBranchingDisjointPlan() {
		// construct the plan
		final String out1Path = "file:///test/1";
		final String out2Path = "file:///test/2";
		final String out3Path = "file:///test/3";
		final String out4Path = "file:///test/4";

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE);
		
		FileDataSink sink1 = new FileDataSink(DummyOutputFormat.class, out1Path, sourceA, "1");
		FileDataSink sink2 = new FileDataSink(DummyOutputFormat.class, out2Path, sourceB, "2");
		FileDataSink sink3 = new FileDataSink(DummyOutputFormat.class, out3Path, sourceA, "3");
		FileDataSink sink4 = new FileDataSink(DummyOutputFormat.class, out4Path, sourceB, "4");
		
		
		List<FileDataSink> sinks = new ArrayList<FileDataSink>();
		sinks.add(sink1);
		sinks.add(sink2);
		sinks.add(sink3);
		sinks.add(sink4);
		
		// return the PACT plan
		Plan plan = new Plan(sinks, "Disjoint plan with multiple data sinks and branches");
		compileNoStats(plan);
	}
	
	@Test
	public void testBranchAfterIteration() {
		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 2");
		
		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(sourceA);
		iteration.setMaximumNumberOfIterations(10);
		
		MapOperator mapper = MapOperator.builder(IdentityMap.class).name("Mapper").input(iteration.getPartialSolution()).build();
		iteration.setNextPartialSolution(mapper);
		
		FileDataSink sink1 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, iteration, "Sink 1");
		
		MapOperator postMap = MapOperator.builder(IdentityMap.class).name("Post Iteration Mapper")
				.input(iteration).build();
		
		FileDataSink sink2 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, postMap, "Sink 2");
		
		List<FileDataSink> sinks = new ArrayList<FileDataSink>();
		sinks.add(sink1);
		sinks.add(sink2);
		
		Plan plan = new Plan(sinks);
		
		try {
			compileNoStats(plan);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testBranchBeforeIteration() {
		FileDataSource source1 = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 1");
		FileDataSource source2 = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 2");
		
		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(source2);
		iteration.setMaximumNumberOfIterations(10);
		
		MapOperator inMap = MapOperator.builder(new IdentityMap())
				                       .input(source1)
				                       .name("In Iteration Map")
				                       .setBroadcastVariable("BC", iteration.getPartialSolution())
				                       .build();
		
		iteration.setNextPartialSolution(inMap);
		
		MapOperator postMap = MapOperator.builder(new IdentityMap())
										 .input(source1)
										 .name("Post Iteration Map")
										 .setBroadcastVariable("BC", iteration)
										 .build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, postMap, "Sink");
		
		Plan plan = new Plan(sink);
		
		try {
			compileNoStats(plan);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * Test to ensure that sourceA is inside as well as outside of the iteration the same
	 * node.
	 *
	 * <pre>
	 *       (SRC A)               (SRC B)
	 *      /       \             /       \
	 *  (SINK 1)   (ITERATION)    |     (SINK 2)
	 *             /        \     /
	 *         (SINK 3)     (CROSS => NEXT PARTIAL SOLUTION)
	 * </pre>
	 */
	@Test
	public void testClosure() {
		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 1");
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 2");

		FileDataSink sink1 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, sourceA, "Sink 1");
		FileDataSink sink2 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, sourceB, "Sink 2");

		BulkIteration iteration = new BulkIteration("Loop");
		iteration.setInput(sourceA);
		iteration.setMaximumNumberOfIterations(10);

		CrossOperator stepFunction = CrossOperator.builder(DummyCrossStub.class).name("StepFunction").
				input1(iteration.getPartialSolution()).
				input2(sourceB).
				build();

		iteration.setNextPartialSolution(stepFunction);

		FileDataSink sink3 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, iteration, "Sink 3");

		List<FileDataSink> sinks = new ArrayList<FileDataSink>();
		sinks.add(sink1);
		sinks.add(sink2);
		sinks.add(sink3);

		Plan plan = new Plan(sinks);

		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * <pre>
	 *       (SRC A)         (SRC B)          (SRC C)
	 *      /       \       /                /       \
	 *  (SINK 1) (DELTA ITERATION)          |     (SINK 2)
	 *             /    |   \               /
	 *         (SINK 3) |   (CROSS => NEXT WORKSET)
	 *                  |             |
	 *                (JOIN => SOLUTION SET DELTA)
	 * </pre>
	 */
	@Test
	public void testClosureDeltaIteration() {
		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 1");
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 2");
		FileDataSource sourceC = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source 3");

		FileDataSink sink1 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, sourceA, "Sink 1");
		FileDataSink sink2 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, sourceC, "Sink 2");

		DeltaIteration iteration = new DeltaIteration(0, "Loop");
		iteration.setInitialSolutionSet(sourceA);
		iteration.setInitialWorkset(sourceB);
		iteration.setMaximumNumberOfIterations(10);

		CrossOperator nextWorkset = CrossOperator.builder(DummyCrossStub.class).name("Next workset").
				input1(iteration.getWorkset()).
				input2(sourceC).
				build();

		JoinOperator solutionSetDelta = JoinOperator.builder(DummyMatchStub.class, LongValue.class,0,0).
				name("Next solution set.").
				input1(nextWorkset).
				input2(iteration.getSolutionSet()).
				build();

		iteration.setNextWorkset(nextWorkset);
		iteration.setSolutionSetDelta(solutionSetDelta);

		FileDataSink sink3 = new FileDataSink(DummyOutputFormat.class, OUT_FILE, iteration, "Sink 3");

		List<FileDataSink> sinks = new ArrayList<FileDataSink>();
		sinks.add(sink1);
		sinks.add(sink2);
		sinks.add(sink3);

		Plan plan = new Plan(sinks);

		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * <pre>
	 *                  +----Iteration-------+
	 *                  |                    |
	 *       /---------< >---------join-----< >---sink
	 *      / (Solution)|           /        |
	 *     /            |          /         |
	 *    /--map-------< >----\   /       /--|
	 *   /     (Workset)|      \ /       /   |
	 * src-map          |     join------/    |
	 *   \              |      /             |
	 *    \             +-----/--------------+
	 *     \                 /
	 *      \--reduce-------/
	 * </pre>
	 */
	@Test
	public void testDeltaIterationWithStaticInput() {
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE, "source");

		MapOperator mappedSource = MapOperator.builder(IdentityMap.class).
				input(source).
				name("Identity mapped source").
				build();

		ReduceOperator reducedSource = ReduceOperator.builder(IdentityReduce.class).
				input(source).
				name("Identity reduce source").
				build();

		DeltaIteration iteration = new DeltaIteration(0,"Loop");
		iteration.setMaximumNumberOfIterations(10);
		iteration.setInitialSolutionSet(source);
		iteration.setInitialWorkset(mappedSource);

		JoinOperator nextWorkset = JoinOperator.builder(DummyNonPreservingMatchStub.class, IntValue.class, 0,0).
				input1(iteration.getWorkset()).
				input2(reducedSource).
				name("Next work set").
				build();

		JoinOperator solutionSetDelta = JoinOperator.builder(DummyNonPreservingMatchStub.class, IntValue.class, 0,
				0).
				input1(iteration.getSolutionSet()).
				input2(nextWorkset).
				name("Solution set delta").
				build();

		iteration.setNextWorkset(nextWorkset);
		iteration.setSolutionSetDelta(solutionSetDelta);

		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, iteration, "Iteration sink");
		List<FileDataSink> sinks = new ArrayList<FileDataSink>();
		sinks.add(sink);

		Plan plan = new Plan(sinks);

		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * <pre>
	 *             +---------Iteration-------+
	 *             |                         |
	 *    /--map--< >----\                   |
	 *   /         |      \         /-------< >---sink
	 * src-map     |     join------/         |
	 *   \         |      /                  |
	 *    \        +-----/-------------------+
	 *     \            /
	 *      \--reduce--/
	 * </pre>
	 */
	@Test
	public void testIterationWithStaticInput() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(100);

			DataSet<Long> source = env.generateSequence(1, 1000000);

			DataSet<Long> mapped = source.map(new IdentityMapper<Long>());

			DataSet<Long> reduced = source.groupBy(new IdentityKeyExtractor<Long>()).reduce(new SelectOneReducer<Long>());

			IterativeDataSet<Long> iteration = mapped.iterate(10);
			iteration.closeWith(
					iteration.join(reduced)
							.where(new IdentityKeyExtractor<Long>())
							.equalTo(new IdentityKeyExtractor<Long>())
							.with(new DummyFlatJoinFunction<Long>()))
					.output(new DiscardingOutputFormat<Long>());

			compileNoStats(env.createProgramPlan());
		}
		catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testBranchingBroadcastVariable() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(100);

		DataSet<String> input1 = env.readTextFile(IN_FILE).name("source1");
		DataSet<String> input2 = env.readTextFile(IN_FILE).name("source2");
		DataSet<String> input3 = env.readTextFile(IN_FILE).name("source3");
		
		DataSet<String> result1 = input1
				.map(new IdentityMapper<String>())
				.reduceGroup(new Top1GroupReducer<String>())
					.withBroadcastSet(input3, "bc");
		
		DataSet<String> result2 = input2
				.map(new IdentityMapper<String>())
				.reduceGroup(new Top1GroupReducer<String>())
					.withBroadcastSet(input3, "bc");
		
		result1.join(result2)
				.where(new IdentityKeyExtractor<String>())
				.equalTo(new IdentityKeyExtractor<String>())
				.with(new RichJoinFunction<String, String, String>() {
					@Override
					public String join(String first, String second) {
						return null;
					}
				})
				.withBroadcastSet(input3, "bc1")
				.withBroadcastSet(input1, "bc2")
				.withBroadcastSet(result1, "bc3")
			.print();
		
		Plan plan = env.createProgramPlan();
		
		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testBCVariableClosure() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> input = env.readTextFile(IN_FILE).name("source1");
		
		DataSet<String> reduced = input
				.map(new IdentityMapper<String>())
				.reduceGroup(new Top1GroupReducer<String>());
		
		
		DataSet<String> initialSolution = input.map(new IdentityMapper<String>()).withBroadcastSet(reduced, "bc");
		
		
		IterativeDataSet<String> iteration = initialSolution.iterate(100);
		
		iteration.closeWith(iteration.map(new IdentityMapper<String>()).withBroadcastSet(reduced, "red"))
				.print();
		
		Plan plan = env.createProgramPlan();
		
		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testMultipleIterations() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(100);
		
		DataSet<String> input = env.readTextFile(IN_FILE).name("source1");
		
		DataSet<String> reduced = input
				.map(new IdentityMapper<String>())
				.reduceGroup(new Top1GroupReducer<String>());
			
		IterativeDataSet<String> iteration1 = input.iterate(100);
		IterativeDataSet<String> iteration2 = input.iterate(20);
		IterativeDataSet<String> iteration3 = input.iterate(17);
		
		iteration1.closeWith(iteration1.map(new IdentityMapper<String>()).withBroadcastSet(reduced, "bc1")).print();
		iteration2.closeWith(iteration2.reduceGroup(new Top1GroupReducer<String>()).withBroadcastSet(reduced, "bc2")).print();
		iteration3.closeWith(iteration3.reduceGroup(new IdentityGroupReducer<String>()).withBroadcastSet(reduced, "bc3")).print();
		
		Plan plan = env.createProgramPlan();
		
		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testMultipleIterationsWithClosueBCVars() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(100);

		DataSet<String> input = env.readTextFile(IN_FILE).name("source1");
			
		IterativeDataSet<String> iteration1 = input.iterate(100);
		IterativeDataSet<String> iteration2 = input.iterate(20);
		IterativeDataSet<String> iteration3 = input.iterate(17);
		
		
		iteration1.closeWith(iteration1.map(new IdentityMapper<String>())).print();
		iteration2.closeWith(iteration2.reduceGroup(new Top1GroupReducer<String>())).print();
		iteration3.closeWith(iteration3.reduceGroup(new IdentityGroupReducer<String>())).print();
		
		Plan plan = env.createProgramPlan();
		
		try{
			compileNoStats(plan);
		}catch(Exception e){
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testBranchesOnlyInBCVariables1() {
		try{
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(100);

			DataSet<Long> input = env.generateSequence(1, 10);
			DataSet<Long> bc_input = env.generateSequence(1, 10);
			
			input
				.map(new IdentityMapper<Long>()).withBroadcastSet(bc_input, "name1")
				.map(new IdentityMapper<Long>()).withBroadcastSet(bc_input, "name2")
				.print();
			
			Plan plan = env.createProgramPlan();
			compileNoStats(plan);
		}
		catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testBranchesOnlyInBCVariables2() {
		try{
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(100);

			DataSet<Tuple2<Long, Long>> input = env.generateSequence(1, 10).map(new Duplicator<Long>()).name("proper input");
			
			DataSet<Long> bc_input1 = env.generateSequence(1, 10).name("BC input 1");
			DataSet<Long> bc_input2 = env.generateSequence(1, 10).name("BC input 1");
			
			DataSet<Tuple2<Long, Long>> joinInput1 =
					input.map(new IdentityMapper<Tuple2<Long,Long>>())
						.withBroadcastSet(bc_input1.map(new IdentityMapper<Long>()), "bc1")
						.withBroadcastSet(bc_input2, "bc2");
			
			DataSet<Tuple2<Long, Long>> joinInput2 =
					input.map(new IdentityMapper<Tuple2<Long,Long>>())
						.withBroadcastSet(bc_input1, "bc1")
						.withBroadcastSet(bc_input2, "bc2");
			
			DataSet<Tuple2<Long, Long>> joinResult = joinInput1
				.join(joinInput2, JoinHint.REPARTITION_HASH_FIRST).where(0).equalTo(1)
				.with(new DummyFlatJoinFunction<Tuple2<Long,Long>>());
			
			input
				.map(new IdentityMapper<Tuple2<Long,Long>>())
					.withBroadcastSet(bc_input1, "bc1")
				.union(joinResult)
				.print();
			
			Plan plan = env.createProgramPlan();
			compileNoStats(plan);
		}
		catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static final class Duplicator<T> implements MapFunction<T, Tuple2<T, T>> {
		
		@Override
		public Tuple2<T, T> map(T value) {
			return new Tuple2<T, T>(value, value);
		}
	}
}
