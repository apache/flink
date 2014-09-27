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


package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plandump.PlanJSONDumpGenerator;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.Assert;
import org.junit.Test;

public abstract class RecordAPITestBase extends AbstractTestBase {

	protected static final int DOP = 4;
	
	protected JobExecutionResult jobExecutionResult;
	
	protected boolean printPlan;
	
	
	public RecordAPITestBase() {
		this(new Configuration());
	}
	
	public RecordAPITestBase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(DOP);
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Methods to create the test program and for pre- and post- test work
	// --------------------------------------------------------------------------------------------

	protected void preSubmit() throws Exception {}

	protected void postSubmit() throws Exception {}
	
	
	public JobExecutionResult getJobExecutionResult() {
		return jobExecutionResult;
	}
	
	
	protected JobGraph getJobGraph() throws Exception {
		Plan p = getTestJob();
		if (p == null) {
			Assert.fail("Error: Cannot obtain Pact plan. Did the thest forget to override either 'getPactPlan()' or 'getJobGraph()' ?");
		}
		
		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(p);
		
		if (printPlan) {
			System.out.println(new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(op)); 
		}

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}
	
	protected Plan getTestJob() {
		return null;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Actual Test Entry Point
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
		
		// submit job
		JobGraph jobGraph = null;
		try {
			jobGraph = getJobGraph();
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Failed to obtain JobGraph!");
		}
		
		Assert.assertNotNull("Obtained null JobGraph", jobGraph);
		
		try {
			JobClient client = this.executor.getJobClient(jobGraph);
			client.setConsoleStreamForReporting(getNullPrintStream());
			this.jobExecutionResult = client.submitJobAndWait();
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Job execution failed!");
		}
		
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
}
