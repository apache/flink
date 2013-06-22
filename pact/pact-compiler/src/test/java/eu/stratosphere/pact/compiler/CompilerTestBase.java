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
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.costs.DefaultCostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.generic.io.FileInputFormat.FileBaseStatistics;

/**
 *
 */
public abstract class CompilerTestBase {

	protected static final String IN_FILE = isWindows() ? "file://c:\\" : "file:///dev/random";
	
	protected static final String OUT_FILE = isWindows() ? "file://c:\\" : "file:///dev/null";
	
	protected static final int DEFAULT_PARALLELISM = 8;
	
	protected static final String DEFAULT_PARALLELISM_STRING = String.valueOf(DEFAULT_PARALLELISM);
	
	private static final String CACHE_KEY = "cachekey";
	
	// ------------------------------------------------------------------------
	
	protected DataStatistics dataStats;
	
	protected PactCompiler withStatsCompiler;
	
	protected PactCompiler noStatsCompiler;
	
	protected InstanceTypeDescription instanceType;
	
	private int statCounter;
	
	// ------------------------------------------------------------------------	
	
	@Before
	public void setup() {
		InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 12345);
		
		this.dataStats = new DataStatistics();
		this.withStatsCompiler = new PactCompiler(this.dataStats, new DefaultCostEstimator(), dummyAddr);
		this.withStatsCompiler.setDefaultDegreeOfParallelism(DEFAULT_PARALLELISM);
		
		this.noStatsCompiler = new PactCompiler(null, new DefaultCostEstimator(), dummyAddr);
		this.noStatsCompiler.setDefaultDegreeOfParallelism(DEFAULT_PARALLELISM);
		
		// create the instance type description
		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, DEFAULT_PARALLELISM * 2);
	}
	
	// ------------------------------------------------------------------------
	
	public OptimizedPlan compileWithStats(Plan p) {
		return this.withStatsCompiler.compile(p, this.instanceType);
	}
	
	public OptimizedPlan compileNoStats(Plan p) {
		return this.noStatsCompiler.compile(p, this.instanceType);
	}
	
	public void setSourceStatistics(GenericDataSource<?> source, long size, float recordWidth) {
		setSourceStatistics(source, new FileBaseStatistics(Long.MAX_VALUE, size, recordWidth));
	}
	
	public void setSourceStatistics(GenericDataSource<?> source, FileBaseStatistics stats) {
		final String key = CACHE_KEY + this.statCounter++;
		this.dataStats.cacheBaseStatistics(stats, key);
		source.setStatisticsKey(key);
	}
	
	// ------------------------------------------------------------------------
	
	private static final boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
}
