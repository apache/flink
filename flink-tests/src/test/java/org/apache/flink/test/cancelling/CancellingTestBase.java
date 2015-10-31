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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.Plan;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import org.junit.After;
import org.junit.Before;

/**
 * 
 */
public abstract class CancellingTestBase extends TestLogger {
	
	private static final Logger LOG = LoggerFactory.getLogger(CancellingTestBase.class);

	private static final int MINIMUM_HEAP_SIZE_MB = 192;
	
	/**
	 * Defines the number of seconds after which an issued cancel request is expected to have taken effect (i.e. the job
	 * is canceled), starting from the point in time when the cancel request is issued.
	 */
	private static final int DEFAULT_CANCEL_FINISHED_INTERVAL = 10 * 1000;

	private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = 1;

	// --------------------------------------------------------------------------------------------
	
	protected ForkableFlinkMiniCluster executor;

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
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, true);

		this.executor = new ForkableFlinkMiniCluster(config);
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
		
	public void runAndCancelJob(Plan plan, final int msecsTillCanceling, int maxTimeTillCanceled) throws Exception {
		try {
			// submit job
			final JobGraph jobGraph = getJobGraph(plan);
			
			final Thread currentThread = Thread.currentThread();
			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
			
			boolean jobSuccessfullyCancelled = false;

			// trigger the cancelling asynchronous
			new Thread() {
				@Override
				public void run() {
					try {
						Thread.sleep(msecsTillCanceling);
						executor.getLeaderGateway(TestingUtils.TESTING_DURATION())
							.tell(new JobManagerMessages.CancelJob(jobGraph.getJobID()));
					}
					catch (Throwable t) {
						error.set(t);
						currentThread.interrupt();
					}
				}
			}.run();

			try {
				executor.submitJobAndWait(jobGraph, false);
			}
			catch (JobCancellationException exception) {
				jobSuccessfullyCancelled = true;
			}
			catch (Exception e) {
				throw new IllegalStateException("Job failed.", e);
			}

			if (!jobSuccessfullyCancelled) {
				throw new IllegalStateException("Job was not successfully cancelled.");
			}
		}
		catch(Exception e) {
			LOG.error("Exception found in runAndCancelJob.", e);
			Assert.fail(StringUtils.stringifyException(e));
		}

	}

	private JobGraph getJobGraph(final Plan plan) throws Exception {
		final Optimizer pc = new Optimizer(new DataStatistics(), this.executor.configuration());
		final OptimizedPlan op = pc.compile(plan);
		final JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	public void setTaskManagerNumSlots(int taskManagerNumSlots) { this.taskManagerNumSlots = taskManagerNumSlots; }

	public int getTaskManagerNumSlots() { return this.taskManagerNumSlots; }
}
