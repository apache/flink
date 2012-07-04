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
		
		MatchContract mat1 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceA, sourceA);
		MatchContract mat2 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceA, mat1);
		MatchContract mat3 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceA, mat2);
		MatchContract mat4 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceA, mat3);
		MatchContract mat5 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceA, mat4);
		
		MapContract ma = new MapContract(IdentityMap.class, sourceA);
		
		MatchContract mat6 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,ma, ma);
		MatchContract mat7 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,ma, mat6);
		MatchContract mat8 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,ma, mat7);
		MatchContract mat9 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,ma, mat8);
		MatchContract mat10 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,ma, mat9);
		
		CoGroupContract co = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,mat5, mat10);

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
		
		CoGroupContract co = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,sourceA, sourceB);
		MapContract ma = new MapContract(IdentityMap.class, co);
		MatchContract mat1 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceB, sourceC);
		MatchContract mat2 = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,ma, mat1);
		ReduceContract r = new ReduceContract(IdentityReduce.class, PactInteger.class, 0, ma);
		CrossContract c = new CrossContract(DummyCrossStub.class, r, mat2);
		
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

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		FileDataSource sourceC = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		
		
		MapContract branchingMap = new MapContract(IdentityMap.class, sourceA);
		ReduceContract branchingReduce = new ReduceContract(IdentityReduce.class, PactInteger.class, 0, branchingMap);
		MatchContract branchingMatch = new MatchContract(DummyMatchStub.class, PactInteger.class, 0, 0,sourceB, sourceC);
		CoGroupContract branchingCoGroup = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,sourceA, sourceB);
		CrossContract branchingCross = new CrossContract(DummyCrossStub.class, branchingReduce, branchingCoGroup);
		
		
		CoGroupContract co1 = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,branchingCross, branchingCross);
		CoGroupContract co2 = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,branchingMap, branchingMatch);
		MapContract ma = new MapContract(IdentityMap.class, co2);
		CoGroupContract co3 = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,ma, branchingMatch);
		CoGroupContract co4 = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,co1, branchingCoGroup);
		CoGroupContract co5 = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,branchingReduce, co3);
		CoGroupContract co6 = new CoGroupContract(DummyCoGroupStub.class, PactInteger.class, 0,0,co4, co5);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, co6);

		// return the PACT plan
		Plan plan = new Plan(sink, "Branching of each contract type");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
	}
	
	
}
