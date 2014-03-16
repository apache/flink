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

package eu.stratosphere.test.util;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.jobgraph.JobGraph;


public abstract class JavaProgramTestBase extends AbstractTestBase {

	private static final int DEFAULT_DEGREE_OF_PARALLELISM = 4;
	
	private JobExecutionResult latestExecutionResult;
	
	private int degreeOfParallelism = DEFAULT_DEGREE_OF_PARALLELISM;
	
	
	public JavaProgramTestBase() {
		this(new Configuration());
	}
	
	public JavaProgramTestBase(Configuration config) {
		super(config);
	}
	
	
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		this.degreeOfParallelism = degreeOfParallelism;
	}
	
	public JobExecutionResult getLatestExecutionResult() {
		return this.latestExecutionResult;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create the test program and for pre- and post- test work
	// --------------------------------------------------------------------------------------------

	protected abstract void testProgram() throws Exception;
	

	protected void preSubmit() throws Exception {}
	
	protected void postSubmit() throws Exception {}
	

	// --------------------------------------------------------------------------------------------
	//  Test entry point
	// --------------------------------------------------------------------------------------------

	@Test
	public void testJob() throws Exception {
		// pre-submit
		try {
			preSubmit();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Pre-submit work caused an error: " + e.getMessage());
		}
		
		// prepare the test environment
		TestEnvironment env = new TestEnvironment(this.executor, this.degreeOfParallelism);
		env.setAsContext();
		
		// call the test program
		try {
			testProgram();
			this.latestExecutionResult = env.latestResult;
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Error while calling the test program: " + e.getMessage());
		}
		
		Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);
		
		// post-submit
		try {
			postSubmit();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Post-submit work caused an error: " + e.getMessage());
		}
	}
	
	private static final class TestEnvironment extends ExecutionEnvironment {

		private final NepheleMiniCluster executor;
		
		private JobExecutionResult latestResult;
		
		
		private TestEnvironment(NepheleMiniCluster executor, int degreeOfParallelism) {
			this.executor = executor;
			setDegreeOfParallelism(degreeOfParallelism);
		}

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			try {
				OptimizedPlan op = compileProgram(jobName);
				
				NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
				JobGraph jobGraph = jgg.compileJobGraph(op);
				
				JobClient client = this.executor.getJobClient(jobGraph);
				client.setConsoleStreamForReporting(AbstractTestBase.getNullPrintStream());
				JobExecutionResult result = client.submitJobAndWait();
				
				this.latestResult = result;
				return result;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Job execution failed!");
				return null;
			}
		}


		@Override
		public String getExecutionPlan() throws Exception {
			OptimizedPlan op = compileProgram("unused");
			
			PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
			return jsonGen.getOptimizerPlanAsJSON(op);
		}
		
		
		private OptimizedPlan compileProgram(String jobName) {
			Plan p = createProgramPlan(jobName);
			p.setDefaultParallelism(getDegreeOfParallelism());
			
			PactCompiler pc = new PactCompiler(new DataStatistics());
			return pc.compile(p);
		}
		
		private void setAsContext() {
			initializeContextEnvironment(this);
		}
	}
}
