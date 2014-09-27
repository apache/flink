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


package org.apache.flink.test.cancelling;

import java.util.Iterator;

import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.JobCancelResult;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobProgressResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.JobEvent;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

/**
 * 
 */
public abstract class CancellingTestBase {
	
	private static final Logger LOG = LoggerFactory.getLogger(CancellingTestBase.class);

	private static final int MINIMUM_HEAP_SIZE_MB = 192;
	
	/**
	 * Defines the number of seconds after which an issued cancel request is expected to have taken effect (i.e. the job
	 * is canceled), starting from the point in time when the cancel request is issued.
	 */
	private static final int DEFAULT_CANCEL_FINISHED_INTERVAL = 10 * 1000;

	private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = 1;

	// --------------------------------------------------------------------------------------------
	
	protected NepheleMiniCluster executor;

	protected int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;
	
	// --------------------------------------------------------------------------------------------
	
	private void verifyJvmOptions() {
		final long heap = Runtime.getRuntime().maxMemory() >> 20;
		Assert.assertTrue("Insufficient java heap space " + heap + "mb - set JVM option: -Xmx" + MINIMUM_HEAP_SIZE_MB
				+ "m", heap > MINIMUM_HEAP_SIZE_MB - 50);
	}

	@Before
	public void startCluster() throws Exception {
		verifyJvmOptions();
		this.executor = new NepheleMiniCluster();
		this.executor.setDefaultOverwriteFiles(true);
		this.executor.setTaskManagerNumSlots(taskManagerNumSlots);
		this.executor.start();
	}

	@After
	public void stopCluster() throws Exception {
		if (this.executor != null) {
			this.executor.stop();
			this.executor = null;
			FileSystem.closeAll();
			System.gc();
		}
	}

	// --------------------------------------------------------------------------------------------

	public void runAndCancelJob(Plan plan, int msecsTillCanceling) throws Exception {
		runAndCancelJob(plan, msecsTillCanceling, DEFAULT_CANCEL_FINISHED_INTERVAL);
	}
		
	public void runAndCancelJob(Plan plan, int msecsTillCanceling, int maxTimeTillCanceled) throws Exception {
		try {
			// submit job
			final JobGraph jobGraph = getJobGraph(plan);

			final long startingTime = System.currentTimeMillis();
			long cancelTime = -1L;
			final JobClient client = this.executor.getJobClient(jobGraph);
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
						case RUNNING:
						case CANCELLING:
						case FAILING:
						case CREATED:
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
			LOG.error("Exception while running runAndCancelJob.", e);
			Assert.fail(StringUtils.stringifyException(e));
			return;
		}
	}

	private JobGraph getJobGraph(final Plan plan) throws Exception {
		final PactCompiler pc = new PactCompiler(new DataStatistics());
		final OptimizedPlan op = pc.compile(plan);
		final NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	public void setTaskManagerNumSlots(int taskManagerNumSlots) { this.taskManagerNumSlots = taskManagerNumSlots; }

	public int getTaskManagerNumSlots() { return this.taskManagerNumSlots; }
}
