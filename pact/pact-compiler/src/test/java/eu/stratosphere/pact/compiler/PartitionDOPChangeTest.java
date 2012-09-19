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

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.costs.DefaultCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.MatchNode;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;

/**
 */
public class PartitionDOPChangeTest {
	
	
	private static final String IN_FILE = "file:///test/file";
	
	private static final String OUT_FILE = "file:///test/output1";
	
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
			this.compiler = new PactCompiler(dataStats, new DefaultCostEstimator(), dummyAddress);
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
	public void testPartitionDoPChange() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE);
		
		ReduceContract redA = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceA)
			.build();
		ReduceContract redB = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceB)
			.build();
		
		MatchContract mat = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(redA)
			.input2(redB)
			.build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, mat);
		
		sourceA.setDegreeOfParallelism(5);
		sourceB.setDegreeOfParallelism(7);
		redA.setDegreeOfParallelism(5);
		redB.setDegreeOfParallelism(7);
		
		mat.setDegreeOfParallelism(5);
		
		sink.setDegreeOfParallelism(5);
		
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Partition on DoP Change");
		
		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
		
		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<OptimizerNode>() {
			
			@Override
			public boolean preVisit(OptimizerNode visitable) {
				if (visitable instanceof MatchNode) {
					int forwardedConnections = 0;
					for (PactConnection inConn : visitable.getIncomingConnections()) {
						if (inConn.getShipStrategy().type() == ShipStrategyType.FORWARD) {
							forwardedConnections++;
						}
					}
					
					Assert.assertTrue("Incompatible shipping strategy chosen for match", forwardedConnections < 2);
					return false;
				}
				return true;
			}
			
			@Override
			public void postVisit(OptimizerNode visitable) {
				// DO NOTHING
			}
		});
	}
	
	
}
