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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
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
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.util.DummyCoGroupStub;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;

/**
 */
public class BranchingPlansCompilerTest {
	
	
	private static final String IN_FILE_1 = "file:///test/file";
	
	private static final String OUT_FILE_1 = "file:///test/output1";
	
	private static final String OUT_FILE_2 = "file:///test/output2";
	
	private static final String OUT_FILE_3 = "file:///test/output3";
	
	private static final int defaultParallelism = 8;
	
	// ------------------------------------------------------------------------
	
	private PactCompiler compiler;
	
	private InstanceTypeDescription instanceType;
	
	// ------------------------------------------------------------------------	
	
	@Before
	public void setup()
	{
		try {
			InetSocketAddress dummyAddress = new InetSocketAddress(InetAddress.getLocalHost(), 12345);
			
			// prepare the statistics
			DataStatistics dataStats = new DataStatistics();
			this.compiler = new PactCompiler(dataStats, new FixedSizeClusterCostEstimator(), dummyAddress);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			Assert.fail("Test setup failed.");
		}
		
		// create the instance type description
		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, defaultParallelism * 2);
		
				
	}
	

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
		
		CoGroupContract co = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(mat5)
			.input2(mat10)
			.build();

		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, co);
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Branching Source Multiple Times");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
	}
	
	@Test
	public void testBranchingWithMultipleDataSinks() {
		// construct the plan

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
		
		FileDataSink sinkA = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, c);
		FileDataSink sinkB = new FileDataSink(DummyOutputFormat.class, OUT_FILE_2, mat2);
		FileDataSink sinkC = new FileDataSink(DummyOutputFormat.class, OUT_FILE_3, mat2);
		
		List<GenericDataSink> sinks = new ArrayList<GenericDataSink>();
		sinks.add(sinkA);
		sinks.add(sinkB);
		sinks.add(sinkC);
		
		// return the PACT plan
		Plan plan = new Plan(sinks, "Branching Plans With Multiple Data Sinks");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
	}
	
	@Test
	public void testBranchEachContractType() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source A");
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source B");
		FileDataSource sourceC = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source C");
		
		
		MapContract branchingMap = MapContract.builder(IdentityMap.class).input(sourceA).build();
		ReduceContract branchingReduce = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(branchingMap)
			.build();
		MatchContract branchingMatch = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(sourceB)
			.input2(sourceC)
			.build();
		branchingMatch.addFirstInput(sourceB);
		branchingMatch.addFirstInput(sourceC);
		CoGroupContract branchingCoGroup = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(sourceA)
			.input2(sourceB)
			.build();
		CrossContract branchingCross = CrossContract.builder(DummyCrossStub.class)
			.input1(branchingReduce)
			.input2(branchingCoGroup)
			.build();
		
		
		CoGroupContract co1 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(branchingCross)
			.input2(branchingCross)
			.build();
		CoGroupContract co2 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(branchingMap)
			.input2(branchingMatch)
			.build();
		MapContract ma = MapContract.builder(IdentityMap.class).input(co2).build();
		CoGroupContract co3 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(ma)
			.input2(branchingMatch)
			.build();
		CoGroupContract co4 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(co1)
			.input2(branchingCoGroup)
			.build();
		CoGroupContract co5 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(branchingReduce)
			.input2(co3)
			.build();
		CoGroupContract co6 = CoGroupContract.builder(DummyCoGroupStub.class, PactInteger.class, 0,0)
			.input1(co4)
			.input2(co5)
			.build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, co6);
		sink.addInput(sourceA);
		sink.addInput(co3);
		sink.addInput(co4);
		sink.addInput(co1);
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Branching of each contract type");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
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
			.build();
		
		MapContract ma1 = MapContract.builder(IdentityMap.class).input(mat1).build();
		ReduceContract r1 = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(ma1)
			.build();
		ReduceContract r2 = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(mat1)
			.build();
		
		MapContract ma2 = MapContract.builder(IdentityMap.class).input(mat1).build();
		
		MapContract ma3 = MapContract.builder(IdentityMap.class).input(ma2).build();
		
		MatchContract mat2 = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(r1)
			.input2(ma2)
			.build();
		mat2.addFirstInput(r2);
		mat2.addFirstInput(ma2);
		mat2.addFirstInput(ma3);
		
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, mat2);
		
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Branching Union");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
	}
	
	
}
