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
package eu.stratosphere.pact.compiler;

import java.net.InetSocketAddress;

import org.junit.Before;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.costs.DefaultCostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.generic.io.FileInputFormat;


/**
 *
 */
public abstract class CompilerTestBase {

	protected static final String IN_FILE_1 = "file:///test/file";
	
	protected static final String OUT_FILE_1 = "file///test/output";
	
	protected static final String CACHE_KEY = "cachekey";
	
	protected static final int defaultParallelism = 8;
	
	// ------------------------------------------------------------------------
	
	protected PactCompiler compiler;
	
	protected InstanceTypeDescription instanceType;
	
	// ------------------------------------------------------------------------	
	
	@Before
	public void setup() {
		InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 12345);
		
		DataStatistics dataStats = new DataStatistics();
		dataStats.cacheBaseStatistics(new FileInputFormat.FileBaseStatistics(1000, 128 * 1024 * 1024, 8.0f), CACHE_KEY);
		
		this.compiler = new PactCompiler(dataStats, new DefaultCostEstimator(), dummyAddr);
		
		// create the instance type description
		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, defaultParallelism * 2);
	}
	
	public OptimizedPlan compile(Plan p) {
		return this.compiler.compile(p, this.instanceType);
	}
}
