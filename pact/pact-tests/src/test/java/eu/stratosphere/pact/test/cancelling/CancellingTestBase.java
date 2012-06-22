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

import static junit.framework.Assert.fail;

import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.Constants;
import eu.stratosphere.pact.test.util.minicluster.ClusterProvider;
import eu.stratosphere.pact.test.util.minicluster.ClusterProviderPool;

/**
 * 
 */
public abstract class CancellingTestBase
{
	private static final Log LOG = LogFactory.getLog(CancellingTestBase.class);

	private static final int MINIMUM_HEAP_SIZE_MB = 192;
	
	/**
	 * Defines the number of seconds after which an issued cancel request is expected to have taken effect (i.e. the job
	 * is canceled), starting from the point in time when the cancel request is issued.
	 */
	private static final int DEFAULT_CANCEL_FINISHED_INTERVAL = 10 * 1000;

	// --------------------------------------------------------------------------------------------
	
	protected ClusterProvider cluster;
	
	// --------------------------------------------------------------------------------------------
	
	private void verifyJvmOptions()
	{
		final long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
		Assert.assertTrue("IPv4 stack required - set JVM option: -Djava.net.preferIPv4Stack=true", "true".equals(System
				.getProperty("java.net.preferIPv4Stack")));
	}

	@Before
	public void startCluster() throws Exception
	{
		verifyJvmOptions();
		LOG.info("######################### starting cluster #########################");
		this.cluster = ClusterProviderPool.getInstance(Constants.DEFAULT_TEST_CONFIG);
	}

	@After
	public void stopCluster() throws Exception
	{
		LOG.info("######################### stopping cluster config #########################");
		cluster.stopCluster();
		ClusterProviderPool.removeInstance(Constants.DEFAULT_TEST_CONFIG);
		FileSystem.closeAll();
		System.gc();
	}

	// --------------------------------------------------------------------------------------------

	public void runAndCancelJob(Plan plan, int msecsTillCanceling) throws Exception
	{
		runAndCancelJob(plan, msecsTillCanceling, DEFAULT_CANCEL_FINISHED_INTERVAL);
	}
		
	public void runAndCancelJob(Plan plan, int msecsTillCanceling, int maxTimeTillCanceled) throws Exception
	{
		try {
			// submit job
			final JobGraph jobGraph = getJobGraph(plan);

			final long startingTime = System.currentTimeMillis();
			long cancelTime = -1L;
			final JobClient client = cluster.getJobClient(jobGraph, null);
			final JobSubmissionResult submissionResult = client.submitJob();
			if (submissionResult.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
				throw new IllegalStateException(submissionResult.getDescription());
			}

			final int interval = client.getRecommendedPollingInterval();
			final long sleep = interval * 1000L;

			Thread.sleep(sleep / 2);

			long lastProcessedEventSequenceNumber = -1L;

			while (true) {

				if (Thread.interrupted()) {
					throw new IllegalStateException("Job client has been interrupted");
				}

				final long now = System.currentTimeMillis();

				if (cancelTime < 0L) {

					// Cancel job
					if (startingTime + msecsTillCanceling < now) {

						LOG.info("Issuing cancel request");

						final JobCancelResult jcr = client.cancelJob();

						if (jcr == null) {
							throw new IllegalStateException("Return value of cancelJob is null!");
						}

						if (jcr.getReturnCode() != AbstractJobResult.ReturnCode.SUCCESS) {
							throw new IllegalStateException(jcr.getDescription());
						}

						// Save when the cancel request has been issued
						cancelTime = now;
					}
				} else {

					// Job has already been canceled
					if (cancelTime + maxTimeTillCanceled < now) {
						throw new IllegalStateException("Cancelling of job took " + (now - cancelTime)
							+ " milliseconds, only " + maxTimeTillCanceled + " milliseconds are allowed");
					}
				}

				final JobProgressResult jobProgressResult = client.getJobProgress();

				if (jobProgressResult == null) {
					throw new IllegalStateException("Returned job progress is unexpectedly null!");
				}

				if (jobProgressResult.getReturnCode() == AbstractJobResult.ReturnCode.ERROR) {
					throw new IllegalStateException("Could not retrieve job progress: "
						+ jobProgressResult.getDescription());
				}

				boolean exitLoop = false;

				final Iterator<AbstractEvent> it = jobProgressResult.getEvents();
				while (it.hasNext()) {

					final AbstractEvent event = it.next();

					// Did we already process that event?
					if (lastProcessedEventSequenceNumber >= event.getSequenceNumber()) {
						continue;
					}

					lastProcessedEventSequenceNumber = event.getSequenceNumber();

					// Check if we can exit the loop
					if (event instanceof JobEvent) {
						final JobEvent jobEvent = (JobEvent) event;
						final JobStatus jobStatus = jobEvent.getCurrentJobStatus();

						switch (jobStatus) {
						case FINISHED:
							throw new IllegalStateException("Job finished successfully");
						case FAILED:
							throw new IllegalStateException("Job failed");
						case CANCELED:
							exitLoop = true;
							break;
						}
					}

					if (exitLoop) {
						break;
					}
				}

				if (exitLoop) {
					break;
				}

				Thread.sleep(sleep);
			}

		} catch (Exception e) {
			LOG.error(e);
			fail(StringUtils.stringifyException(e));
			return;
		}
	}

	private JobGraph getJobGraph(final Plan plan) throws Exception
	{
		final PactCompiler pc = new PactCompiler();
		final OptimizedPlan op = pc.compile(plan);
		final JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}
}
