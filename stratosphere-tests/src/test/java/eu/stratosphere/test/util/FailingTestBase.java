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

import org.junit.BeforeClass;

import junit.framework.Assert;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.test.util.minicluster.ClusterProvider;
import eu.stratosphere.util.LogUtils;

/**
 * Base class for integration tests which test whether the system recovers from failed executions.
 *
 */
public abstract class FailingTestBase extends TestBase {


	public FailingTestBase(Configuration testConfig) {
		super(testConfig);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @param config
	 * @param clusterConfig
	 */
	public FailingTestBase(Configuration config, String clusterConfig) {
		super(config,clusterConfig);
	}
	
	@BeforeClass
	public static void initLogging() {
		LogUtils.initializeDefaultTestConsoleLogger();
	}
	
	
	/**
	 * Returns the {@link JobGraph} of the failing job. 
	 * 
	 * @return The JobGraph of the failing job.
	 * @throws Exception
	 */
	abstract protected JobGraph getFailingJobGraph() throws Exception;
	
	/**
	 * Returns the path to the jar-file of the failing job.
	 * 
	 * @return Path to the jar-file of the failing job.
	 */
	protected String getFailingJarFilePath() {
		return null;
	}
	
	/**
	 * Returns the timeout for the execution of both (the failing and the working) job in seconds.
	 * 
	 * @return Timeout for the execution of both jobs in seconds.
	 */
	abstract protected int getTimeout();
	
	/**
	 * Tests that both jobs, the failing and the working one, are handled correctly.
	 * The first (failing) job must be canceled and the Nephele client must report the failure.
	 * The second (working) job must finish successfully and compute the correct result.
	 * A timeout waits for the successful return for the Nephele client. In case of a deadlock 
	 * (or too small value for timeout) the time runs out and this test fails. 
	 * 
	 */
	@Override
	public void testJob() throws Exception {
		// pre-submit (initialize data for job)
		preSubmit();

		// init submission thread
		SubmissionThread st = new SubmissionThread(Thread.currentThread(), cluster, getFailingJobGraph(), getJobGraph());
		// start submission thread
		st.start();
		
		boolean successBeforeTimeout = false;
		try {
			// wait for timeout
			Thread.sleep(getTimeout()*1000);
		} catch(InterruptedException ie) {
			// thread was interrupted by submission thread
			// both first job was handled correctly, second job finished
			successBeforeTimeout = true;
		}
		
		// assert that second job finished correctly
		Assert.assertTrue("Jobs did not finish within timeout", successBeforeTimeout);
		
		// post-submit (check result of second job)
		postSubmit();
	}
	
	/**
	 * Thread for submitting both jobs sequentially to the test cluster.
	 * First, the failing job is submitted. The working job is submitted after the Nephele client returns 
	 * from the call of its submitJobAndWait() method. 
	 */
	private class SubmissionThread extends Thread {

		// reference to the timeout thread
		private Thread timeoutThread;
		// cluster to submit the job to.
		private ClusterProvider cluster;
		// job graph of the failing job (submitted first)
		private JobGraph failingJob;
		// job graph of the working job (submitted after return from failing job)
		private JobGraph job;
		
		/**
		 * 
		 * @param timeoutThread Reference to timeout thread
		 * @param cluster       cluster to submit both jobs to
		 * @param failingJob    job graph of the failing job (submitted first)
		 * @param job           job graph of the working job (submitted after return from failing job)
		 */
		public SubmissionThread(Thread timeoutThread, ClusterProvider cluster, JobGraph failingJob, JobGraph job) {
			this.timeoutThread = timeoutThread;
			this.cluster = cluster;
			this.failingJob = failingJob;
			this.job = job;
		}
		
		/**
		 * Submits the failing and the working job sequentially to the cluster.
		 * As soon as the second job finishes, the timeout thread is interrupted and this thread closed.
		 */
		@Override
		public void run() {
			boolean jobFailed = false;
			try {
				// submit failing job
				final JobClient client = this.cluster.getJobClient(this.failingJob, getFailingJarFilePath());
				client.submitJobAndWait();
			} catch(JobExecutionException jee) {
				// check that job execution failed
				jobFailed = true;
			} catch(Exception e) {
				Assert.fail(e.getMessage());
			}
			
			// assert that first job failed
			Assert.assertTrue("First job must fail", jobFailed);
			
			try {
				// submit working job
				final JobClient client = this.cluster.getJobClient(this.job, getFailingJarFilePath());
				client.submitJobAndWait();
			} catch (Exception e) {
				// this job should not fail
				Assert.fail(e.getMessage());
			}
			
			// interrupt timeout thread
			timeoutThread.interrupt();
		}
		
	}
	
}
