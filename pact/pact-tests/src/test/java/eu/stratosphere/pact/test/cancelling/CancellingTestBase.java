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

package eu.stratosphere.pact.test.cancelling;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.TestBase;

/**
 * 
 */
public abstract class CancellingTestBase extends TestBase
{
	private static final Log LOG = LogFactory.getLog(CancellingTestBase.class);
	
	// --------------------------------------------------------------------------------------------
	
	public CancellingTestBase() {
		this(new Configuration());
	}
	
	public CancellingTestBase(final Configuration config) {
		super(config);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testJob() throws Exception
	{
		// pre-submit
		preSubmit();

		// submit job
		final JobGraph jobGraph;
		try {
			jobGraph = getJobGraph();
		} catch(Exception e) {
			LOG.error(e);
			Assert.fail("Failed to obtain JobGraph!");
			return;
		}
		
		final JobClient client;
		final JobSubmissionResult submissionResult;
		final long startingTime = System.currentTimeMillis();
		
		try {
			client = cluster.getJobClient(jobGraph, getJarFilePath());
			client.submitJob();
		}
		catch(Exception e) {
			LOG.error(e);
			Assert.fail("Job execution failed!");
			return;
		}
		
		// check progress till termination
		client.getJobProgress();
		
		// post-submit
		postSubmit();
	}
	

	@Override
	protected JobGraph getJobGraph() throws Exception
	{
		final Plan testPlan = getTestPlan();
		testPlan.setDefaultParallelism(4);
		
		final PactCompiler pc = new PactCompiler();
		final OptimizedPlan op = pc.compile(testPlan);
		final JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.test.util.TestBase#preSubmit()
	 */
	@Override
	protected void preSubmit() throws Exception {
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.test.util.TestBase#postSubmit()
	 */
	@Override
	protected void postSubmit() throws Exception {
	}

	protected abstract Plan getTestPlan() throws Exception;
}
