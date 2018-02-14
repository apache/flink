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

import org.apache.flink.api.common.Plan;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import static org.apache.flink.runtime.messages.JobManagerMessages.CancellationFailure;
import static org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;

/**
 * Base class for testing job cancellation.
 */
public abstract class CancelingTestBase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(CancelingTestBase.class);

	private static final int MINIMUM_HEAP_SIZE_MB = 192;

	/**
	 * Defines the number of seconds after which an issued cancel request is expected to have taken effect (i.e. the job
	 * is canceled), starting from the point in time when the cancel request is issued.
	 */
	private static final int DEFAULT_CANCEL_FINISHED_INTERVAL = 10 * 1000;

	private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = 1;

	// --------------------------------------------------------------------------------------------

	protected LocalFlinkMiniCluster executor;

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
		config.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 4);
		config.setString(AkkaOptions.ASK_TIMEOUT, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		config.setInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE, 4096);
		config.setInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS, 2048);

		this.executor = new LocalFlinkMiniCluster(config, false);
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

	public void runAndCancelJob(Plan plan, final int msecsTillCanceling, int maxTimeTillCanceled) throws Exception {
		try {
			// submit job
			final JobGraph jobGraph = getJobGraph(plan);

			executor.submitJobDetached(jobGraph);

			// Wait for the job to make some progress and then cancel
			JobManagerActorTestUtils.waitForJobStatus(jobGraph.getJobID(), JobStatus.RUNNING,
					executor.getLeaderGateway(TestingUtils.TESTING_DURATION()),
					TestingUtils.TESTING_DURATION());

			Thread.sleep(msecsTillCanceling);

			FiniteDuration timeout = new FiniteDuration(maxTimeTillCanceled, TimeUnit.MILLISECONDS);

			ActorGateway jobManager = executor.getLeaderGateway(TestingUtils.TESTING_DURATION());

			Future<Object> ask = jobManager.ask(new CancelJob(jobGraph.getJobID()), timeout);

			Object result = Await.result(ask, timeout);

			if (result instanceof CancellationSuccess) {
				// all good
			} else if (result instanceof CancellationFailure) {
				// Failure
				CancellationFailure failure = (CancellationFailure) result;
				throw new Exception("Failed to cancel job with ID " + failure.jobID() + ".",
						failure.cause());
			} else {
				throw new Exception("Unexpected response to cancel request: " + result);
			}

			// Wait for the job to be cancelled
			JobManagerActorTestUtils.waitForJobStatus(jobGraph.getJobID(), JobStatus.CANCELED,
					executor.getLeaderGateway(TestingUtils.TESTING_DURATION()),
					TestingUtils.TESTING_DURATION());
		}
		catch (Exception e) {
			LOG.error("Exception found in runAndCancelJob.", e);
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private JobGraph getJobGraph(final Plan plan) throws Exception {
		final Optimizer pc = new Optimizer(new DataStatistics(), this.executor.configuration());
		final OptimizedPlan op = pc.compile(plan);
		final JobGraphGenerator jgg = new JobGraphGenerator();
		return jgg.compileJobGraph(op);
	}

	public void setTaskManagerNumSlots(int taskManagerNumSlots) {
		this.taskManagerNumSlots = taskManagerNumSlots;
	}

	public int getTaskManagerNumSlots() {
		return this.taskManagerNumSlots;
	}
}
