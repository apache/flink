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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.api.Job;
import eu.stratosphere.api.operators.BulkIteration;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.operators.GenericDataSink;
import eu.stratosphere.api.record.operators.CoGroupOperator;
import eu.stratosphere.api.record.operators.CrossOperator;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyCoGroupStub;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.types.PactInteger;

/**
 */
public class BranchingPlansCompilerTest extends CompilerTestBase {
	
	
	@Test
	public void testCostComputationWithMultipleDataSinks() {
		final int SINKS = 5;
	
		try {
			List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
	
			// construct the plan
			final String out1Path = "file:///test/1";
			final String out2Path = "file:///test/2";
	
			FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
	
			MapOperator mapA = MapOperator.builder(IdentityMap.class).input(sourceA).name("Map A").build();
			MapOperator mapC = MapOperator.builder(IdentityMap.class).input(mapA).name("Map C").build();
	
			FileDataSink[] sinkA = new FileDataSink[SINKS];
			FileDataSink[] sinkB = new FileDataSink[SINKS];
			for (int sink = 0; sink < SINKS; sink++) {
				sinkA[sink] = new FileDataSink(DummyOutputFormat.class, out1Path, mapA, "Sink A:" + sink);
				sinks.add(sinkA[sink]);
	
				sinkB[sink] = new FileDataSink(DummyOutputFormat.class, out2Path, mapC, "Sink B:" + sink);
				sinks.add(sinkB[sink]);
			}
	
			// return the PACT plan
			Job plan = new Job(sinks, "Plans With Multiple Data Sinks");
	
			OptimizedPlan oPlan = compileNoStats(plan);
	
			// ---------- compile plan to nephele job graph to verify that no error is thrown ----------
	
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
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
	@Test
	public void testBranchingWithMultipleDataSinks2() {
		try {
			// construct the plan
			final String out1Path = "file:///test/1";
			final String out2Path = "file:///test/2";
			final String out3Path = "file:///test/3";
	
			FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);

			MapOperator mapA = MapOperator.builder(IdentityMap.class).input(sourceA).name("Map A").build();
			MapOperator mapB = MapOperator.builder(IdentityMap.class).input(mapA).name("Map B").build();
			MapOperator mapC = MapOperator.builder(IdentityMap.class).input(mapA).name("Map C").build();
			
			FileDataSink sinkA = new FileDataSink(DummyOutputFormat.class, out1Path, mapB, "Sink A");
			FileDataSink sinkB = new FileDataSink(DummyOutputFormat.class, out2Path, mapC, "Sink B");
			FileDataSink sinkC = new FileDataSink(DummyOutputFormat.class, out3Path, mapC, "Sink C");
			
			List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
			sinks.add(sinkA);
			sinks.add(sinkB);
			sinks.add(sinkC);
			
			// return the PACT plan
			Job plan = new Job(sinks, "Plans With Multiple Data Sinks");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			// ---------- check the optimizer plan ----------
			
			// number of sinks
			Assert.assertEquals("Wrong number of data sinks.", 3, oPlan.getDataSinks().size());
			
			// sinks contain all sink paths
			Set<String> allSinks = new HashSet<String>();
			allSinks.add(out1Path);
			allSinks.add(out2Path);
			allSinks.add(out3Path);
			
			for (SinkPlanNode n : oPlan.getDataSinks()) {
				String path = ((FileDataSink) n.getSinkNode().getPactContract()).getFilePath();
				Assert.assertTrue("Invalid data sink.", allSinks.remove(path));
			}
			
			// ---------- compile plan to nephele job graph to verify that no error is thrown ----------
			
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
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
			// construct the plan
			FileDataSource sourceA = new FileDataSource(new DummyInputFormat(), IN_FILE);
			
			JoinOperator mat1 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceA)
				.input2(sourceA)
				.build();
			JoinOperator mat2 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceA)
				.input2(mat1)
				.build();
			JoinOperator mat3 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceA)
				.input2(mat2)
				.build();
			JoinOperator mat4 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceA)
				.input2(mat3)
				.build();
			JoinOperator mat5 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceA)
				.input2(mat4)
				.build();
			
			MapOperator ma = MapOperator.builder(new IdentityMap()).input(sourceA).build();
			
			JoinOperator mat6 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(ma)
				.input2(ma)
				.build();
			JoinOperator mat7 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(ma)
				.input2(mat6)
				.build();
			JoinOperator mat8 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(ma)
				.input2(mat7)
				.build();
			JoinOperator mat9 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(ma)
				.input2(mat8)
				.build();
			JoinOperator mat10 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(ma)
				.input2(mat9)
				.build();
			
			CoGroupOperator co = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0, 0)
				.input1(mat5)
				.input2(mat10)
				.build();
	
			FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, co);
			
			// return the PACT plan
			Job plan = new Job(sink, "Branching Source Multiple Times");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			
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
			// construct the plan
			final String out1Path = "file:///test/1";
			final String out2Path = "file:///test/2";
			final String out3Path = "file:///test/3";
	
			FileDataSource sourceA = new FileDataSource(new DummyInputFormat(), IN_FILE);
			FileDataSource sourceB = new FileDataSource(new DummyInputFormat(), IN_FILE);
			FileDataSource sourceC = new FileDataSource(new DummyInputFormat(), IN_FILE);
			
			CoGroupOperator co = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(sourceA)
				.input2(sourceB)
				.build();
			MapOperator ma = MapOperator.builder(new IdentityMap()).input(co).build();
			JoinOperator mat1 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceB)
				.input2(sourceC)
				.build();
			JoinOperator mat2 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(ma)
				.input2(mat1)
				.build();
			ReduceOperator r = ReduceOperator.builder(new IdentityReduce(), PactInteger.class, 0)
				.input(ma)
				.build();
			CrossOperator c = CrossOperator.builder(new DummyCrossStub())
				.input1(r)
				.input2(mat2)
				.build();
			
			FileDataSink sinkA = new FileDataSink(new DummyOutputFormat(), out1Path, c);
			FileDataSink sinkB = new FileDataSink(new DummyOutputFormat(), out2Path, mat2);
			FileDataSink sinkC = new FileDataSink(new DummyOutputFormat(), out3Path, mat2);
			
			List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
			sinks.add(sinkA);
			sinks.add(sinkB);
			sinks.add(sinkC);
			
			// return the PACT plan
			Job plan = new Job(sinks, "Branching Plans With Multiple Data Sinks");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			// ---------- check the optimizer plan ----------
			
			// number of sinks
			Assert.assertEquals("Wrong number of data sinks.", 3, oPlan.getDataSinks().size());
			
			// sinks contain all sink paths
			Set<String> allSinks = new HashSet<String>();
			allSinks.add(out1Path);
			allSinks.add(out2Path);
			allSinks.add(out3Path);
			
			for (SinkPlanNode n : oPlan.getDataSinks()) {
				String path = ((FileDataSink) n.getSinkNode().getPactContract()).getFilePath();
				Assert.assertTrue("Invalid data sink.", allSinks.remove(path));
			}
			
			// ---------- compile plan to nephele job graph to verify that no error is thrown ----------
			
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testBranchEachContractType() {
		try {
			// construct the plan
			FileDataSource sourceA = new FileDataSource(new DummyInputFormat(), "file:///test/file1", "Source A");
			FileDataSource sourceB = new FileDataSource(new DummyInputFormat(), "file:///test/file2", "Source B");
			FileDataSource sourceC = new FileDataSource(new DummyInputFormat(), "file:///test/file3", "Source C");
			
			MapOperator map1 = MapOperator.builder(new IdentityMap()).input(sourceA).name("Map 1").build();
			
			ReduceOperator reduce1 = ReduceOperator.builder(new IdentityReduce(), PactInteger.class, 0)
				.input(map1)
				.name("Reduce 1")
				.build();
			
			JoinOperator match1 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(sourceB, sourceB, sourceC)
				.input2(sourceC)
				.name("Match 1")
				.build();
			;
			CoGroupOperator cogroup1 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(sourceA)
				.input2(sourceB)
				.name("CoGroup 1")
				.build();
			
			CrossOperator cross1 = CrossOperator.builder(new DummyCrossStub())
				.input1(reduce1)
				.input2(cogroup1)
				.name("Cross 1")
				.build();
			
			
			CoGroupOperator cogroup2 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(cross1)
				.input2(cross1)
				.name("CoGroup 2")
				.build();
			
			CoGroupOperator cogroup3 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(map1)
				.input2(match1)
				.name("CoGroup 3")
				.build();
			
			
			MapOperator map2 = MapOperator.builder(new IdentityMap()).input(cogroup3).name("Map 2").build();
			
			CoGroupOperator cogroup4 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(map2)
				.input2(match1)
				.name("CoGroup 4")
				.build();
			
			CoGroupOperator cogroup5 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(cogroup2)
				.input2(cogroup1)
				.name("CoGroup 5")
				.build();
			
			CoGroupOperator cogroup6 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(reduce1)
				.input2(cogroup4)
				.name("CoGroup 6")
				.build();
			
			CoGroupOperator cogroup7 = CoGroupOperator.builder(new DummyCoGroupStub(), PactInteger.class, 0,0)
				.input1(cogroup5)
				.input2(cogroup6)
				.name("CoGroup 7")
				.build();
			
			FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, cogroup7);
	//		sink.addInput(sourceA);
	//		sink.addInput(co3);
	//		sink.addInput(co4);
	//		sink.addInput(co1);
			
			// return the PACT plan
			Job plan = new Job(sink, "Branching of each contract type");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			
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
			
			JoinOperator mat1 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(source1)
				.input2(source2)
				.name("Match 1")
				.build();
			
			MapOperator ma1 = MapOperator.builder(new IdentityMap()).input(mat1).name("Map1").build();
			
			ReduceOperator r1 = ReduceOperator.builder(new IdentityReduce(), PactInteger.class, 0)
				.input(ma1)
				.name("Reduce 1")
				.build();
			
			ReduceOperator r2 = ReduceOperator.builder(new IdentityReduce(), PactInteger.class, 0)
				.input(mat1)
				.name("Reduce 2")
				.build();
			
			MapOperator ma2 = MapOperator.builder(new IdentityMap()).input(mat1).name("Map 2").build();
			
			MapOperator ma3 = MapOperator.builder(new IdentityMap()).input(ma2).name("Map 3").build();
			
			JoinOperator mat2 = JoinOperator.builder(new DummyMatchStub(), PactInteger.class, 0, 0)
				.input1(r1, r2, ma2, ma3)
				.input2(ma2)
				.name("Match 2")
				.build();
			mat2.setParameter(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.HINT_LOCAL_STRATEGY_MERGE);
			
			FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, mat2);
			
			
			// return the PACT plan
			Job plan = new Job(sink, "Branching Union");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			
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
			
			List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
			sinks.add(sinkA);
			sinks.add(sinkB);
			
			// return the PACT plan
			Job plan = new Job(sinks, "Plans With Multiple Data Sinks");
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			// ---------- check the optimizer plan ----------
			
			// number of sinks
			Assert.assertEquals("Wrong number of data sinks.", 2, oPlan.getDataSinks().size());
			
			// sinks contain all sink paths
			Set<String> allSinks = new HashSet<String>();
			allSinks.add(out1Path);
			allSinks.add(out2Path);
			
			for (SinkPlanNode n : oPlan.getDataSinks()) {
				String path = ((FileDataSink) n.getSinkNode().getPactContract()).getFilePath();
				Assert.assertTrue("Invalid data sink.", allSinks.remove(path));
			}
			
			// ---------- compile plan to nephele job graph to verify that no error is thrown ----------
			
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * 
	 * <pre>
	 *           (SINK A)    (SINK B)
	 *             /           /
	 *         (SRC A)     (SRC B)
	 * </pre>
	 */
	@Test
	public void testSimpleDisjointPlan() {
		// construct the plan
		final String out1Path = "file:///test/1";
		final String out2Path = "file:///test/2";

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE);
		
		FileDataSink sinkA = new FileDataSink(DummyOutputFormat.class, out1Path, sourceA);
		FileDataSink sinkB = new FileDataSink(DummyOutputFormat.class, out2Path, sourceB);
		
		List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
		sinks.add(sinkA);
		sinks.add(sinkB);
		
		// return the PACT plan
		Job plan = new Job(sinks, "Disjoint plan with multiple data sinks");
		
		try {
			compileNoStats(plan);
			Assert.fail("Plan must not be compilable, it contains disjoint sub-plans.");
		}
		catch (Exception ex) {
			// as expected
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
//	@Test (Deactivated for now because of unsupported feature)
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
		
		
		List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
		sinks.add(sink1);
		sinks.add(sink2);
		sinks.add(sink3);
		sinks.add(sink4);
		
		// return the PACT plan
		Job plan = new Job(sinks, "Disjoint plan with multiple data sinks and branches");
		
		try {
			compileNoStats(plan);
			Assert.fail("Plan must not be compilable, it contains disjoint sub-plans.");
		}
		catch (Exception ex) {
			// as expected
		}
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
		
		List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
		sinks.add(sink1);
		sinks.add(sink2);
		
		Job plan = new Job(sinks);
		
		try {
			compileNoStats(plan);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
