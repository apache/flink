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

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.test.util.TestBase;

/**
 * 
 */
public abstract class CancellingTestBase extends TestBase {

	private static final Log LOG = LogFactory.getLog(CancellingTestBase.class);

	/**
	 * Defines the number of seconds after which the cancel request is issued to the job manager, starting from the
	 * point in time when the job is submitted.
	 */
	private static final int CANCEL_INITIATED_INTERVAL = 10;

	/**
	 * Defines the number of seconds after which an issued cancel request is expected to have taken effect (i.e. the job
	 * is canceled), starting from the point in time when the cancel request is issued.
	 */
	private static final int CANCEL_FINISHED_INTERVAL = 10;

	// --------------------------------------------------------------------------------------------

	public CancellingTestBase() {
		this(new Configuration());
	}

	public CancellingTestBase(final Configuration config) {
		super(config);
	}

	// --------------------------------------------------------------------------------------------

	@Test
	public void testJob() throws Exception {
		// pre-submit
		preSubmit();

		try {
			// submit job
			final JobGraph jobGraph = getJobGraph();

			final long startingTime = System.currentTimeMillis();
			long cancelTime = -1L;
			final JobClient client = cluster.getJobClient(jobGraph, getJarFilePath());
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
					if (startingTime + (CANCEL_INITIATED_INTERVAL * 1000L) < now) {

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
					if (cancelTime + (CANCEL_FINISHED_INTERVAL * 1000L) < now) {
						throw new IllegalStateException("Cancelling of job took " + (now - cancelTime)
							+ " milliseconds, only " + CANCEL_FINISHED_INTERVAL + " seconds are allowed");
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

		// post-submit
		postSubmit();
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		final Plan testPlan = getTestPlan();
		testPlan.setDefaultParallelism(4);

		final PactCompiler pc = new PactCompiler();
		final OptimizedPlan op = pc.compile(testPlan);
		final JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	// --------------------------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.test.util.TestBase#preSubmit()
	 */
	@Override
	protected void preSubmit() throws Exception {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.test.util.TestBase#postSubmit()
	 */
	@Override
	protected void postSubmit() throws Exception {
	}

	protected abstract Plan getTestPlan() throws Exception;
}
