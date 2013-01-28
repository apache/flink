/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyCoGroupStub;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;

/**
 */
public class BranchingPlansCompilerTest extends CompilerTestBase {

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
		// construct the plan

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		
		MatchContract mat1 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceA)
			.input2(sourceA)
			.build();
		MatchContract mat2 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceA)
			.input2(mat1)
			.build();
		MatchContract mat3 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceA)
			.input2(mat2)
			.build();
		MatchContract mat4 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceA)
			.input2(mat3)
			.build();
		MatchContract mat5 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceA)
			.input2(mat4)
			.build();
		
		MapContract ma = MapContract.builder(IdentityMap.class).input(sourceA).build();
		
		MatchContract mat6 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(ma)
			.input2(ma)
			.build();
		MatchContract mat7 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(ma)
			.input2(mat6)
			.build();
		MatchContract mat8 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(ma)
			.input2(mat7)
			.build();
		MatchContract mat9 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(ma)
			.input2(mat8)
			.build();
		MatchContract mat10 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(ma)
			.input2(mat9)
			.build();
		
		CoGroupContract co = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0, 0)
			.input1(mat5)
			.input2(mat10)
			.build();

		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, co);
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Branching Source Multiple Times");
		
		OptimizedPlan oPlan = compile(plan);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
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
		// construct the plan
		final String out1Path = "file:///test/1";
		final String out2Path = "file:///test/2";
		final String out3Path = "file:///test/3";

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		FileDataSource sourceC = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		
		CoGroupContract co = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(sourceA)
			.input2(sourceB)
			.build();
		MapContract ma = MapContract.builder(IdentityMap.class).input(co).build();
		MatchContract mat1 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceB)
			.input2(sourceC)
			.build();
		MatchContract mat2 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(ma)
			.input2(mat1)
			.build();
		ReduceContract r = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(ma)
			.build();
		CrossContract c = CrossContract.builder(DummyCrossStub.class)
			.input1(r)
			.input2(mat2)
			.build();
		
		FileDataSink sinkA = new FileDataSink(DummyOutputFormat.class, out1Path, c);
		FileDataSink sinkB = new FileDataSink(DummyOutputFormat.class, out2Path, mat2);
		FileDataSink sinkC = new FileDataSink(DummyOutputFormat.class, out3Path, mat2);
		
		List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
		sinks.add(sinkA);
		sinks.add(sinkB);
		sinks.add(sinkC);
		
		// return the PACT plan
		Plan plan = new Plan(sinks, "Branching Plans With Multiple Data Sinks");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
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
	}
	
	@Test
	public void testBranchEachContractType() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, "file:///test/file1", "Source A");
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, "file:///test/file2", "Source B");
		FileDataSource sourceC = new FileDataSource(DummyInputFormat.class, "file:///test/file3", "Source C");
		
		MapContract map1 = MapContract.builder(IdentityMap.class).input(sourceA).name("Map 1").build();
		
		ReduceContract reduce1 = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(map1)
			.name("Reduce 1")
			.build();
		
		MatchContract match1 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceB, sourceB, sourceC)
			.input2(sourceC)
			.name("Match 1")
			.build();
		;
		CoGroupContract cogroup1 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(sourceA)
			.input2(sourceB)
			.name("CoGroup 1")
			.build();
		
		CrossContract cross1 = CrossContract.builder(DummyCrossStub.class)
			.input1(reduce1)
			.input2(cogroup1)
			.name("Cross 1")
			.build();
		
		
		CoGroupContract cogroup2 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(cross1)
			.input2(cross1)
			.name("CoGroup 2")
			.build();
		
		CoGroupContract cogroup3 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(map1)
			.input2(match1)
			.name("CoGroup 3")
			.build();
		
		
		MapContract map2 = MapContract.builder(IdentityMap.class).input(cogroup3).name("Map 2").build();
		
		CoGroupContract cogroup4 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(map2)
			.input2(match1)
			.name("CoGroup 4")
			.build();
		
		CoGroupContract cogroup5 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(cogroup2)
			.input2(cogroup1)
			.name("CoGroup 5")
			.build();
		
		CoGroupContract cogroup6 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(reduce1)
			.input2(cogroup4)
			.name("CoGroup 6")
			.build();
		
		CoGroupContract cogroup7 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(cogroup5)
			.input2(cogroup6)
			.name("CoGroup 7")
			.build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, cogroup7);
//		sink.addInput(sourceA);
//		sink.addInput(co3);
//		sink.addInput(co4);
//		sink.addInput(co1);
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Branching of each contract type");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
	}
	

	@Test
	public void testBranchingUnion() {
		// construct the plan

		FileDataSource source1 = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		FileDataSource source2 = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		
		MatchContract mat1 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(source1)
			.input2(source2)
			.name("Match 1")
			.build();
		
		MapContract ma1 = MapContract.builder(IdentityMap.class).input(mat1).name("Map1").build();
		
		ReduceContract r1 = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(ma1)
			.name("Reduce 1")
			.build();
		
		ReduceContract r2 = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(mat1)
			.name("Reduce 2")
			.build();
		
		MapContract ma2 = MapContract.builder(IdentityMap.class).input(mat1).name("Map 2").build();
		
		MapContract ma3 = MapContract.builder(IdentityMap.class).input(ma2).name("Map 3").build();
		
		MatchContract mat2 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(r1, r2, ma2, ma3)
			.input2(ma2)
			.name("Match 2")
			.build();
		mat2.setParameter(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.HINT_LOCAL_STRATEGY_MERGE);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, mat2);
		
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Branching Union");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
	}
}
