/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.failing;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.function.Predicate;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.junit.Assert.fail;

/**
 * Tests for failing job submissions.
 */
public class JobSubmissionFailsITCase extends TestLogger {

	private static final int NUM_TM = 2;
	private static final int NUM_SLOTS = 20;

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(NUM_TM)
			.setNumberSlotsPerTaskManager(NUM_SLOTS / NUM_TM)
			.build());

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));

		// to accommodate for 10 netty arenas (NUM_SLOTS / NUM_TM) x 16Mb (NettyBufferPool.ARENA_SIZE)
		config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("256m"));

		return config;
	}

	private static JobGraph getWorkingJobGraph() {
		final JobVertex jobVertex = new JobVertex("Working job vertex.");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		return new JobGraph("Working testing job", jobVertex);
	}


	// --------------------------------------------------------------------------------------------

	@Test
	public void testExceptionInInitializeOnMaster() throws Exception {
		final JobVertex failingJobVertex = new FailingJobVertex("Failing job vertex");
		failingJobVertex.setInvokableClass(NoOpInvokable.class);

		final JobGraph failingJobGraph = new JobGraph("Failing testing job", failingJobVertex);
		runJobSubmissionTest(failingJobGraph, e ->
			ExceptionUtils.findThrowable(
				e,
				candidate -> "Test exception.".equals(candidate.getMessage()))
				.isPresent());
	}

	@Test
	public void testSubmitEmptyJobGraph() throws Exception {
		final JobGraph jobGraph = new JobGraph("Testing job");
		runJobSubmissionTest(
			jobGraph,
			e ->
				ExceptionUtils.findThrowable(
					e,
					throwable -> throwable.getMessage() != null && throwable.getMessage().contains("empty"))
					.isPresent());
	}

	@Test
	public void testMissingJarBlob() throws Exception {
		final JobGraph jobGraph = getJobGraphWithMissingBlobKey();
		runJobSubmissionTest(jobGraph, e -> ExceptionUtils.findThrowable(e, IOException.class).isPresent());
	}

	private void runJobSubmissionTest(JobGraph jobGraph, Predicate<Exception> failurePredicate) throws Exception {
		ClusterClient<?> client = MINI_CLUSTER_RESOURCE.getClusterClient();

		try {
			submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());
			fail("Job submission should have thrown an exception.");
		} catch (Exception e) {
			if (!failurePredicate.test(e)) {
				throw e;
			}
		}

		submitJobAndWaitForResult(client, getWorkingJobGraph(), getClass().getClassLoader());
	}

	@Nonnull
	private static JobGraph getJobGraphWithMissingBlobKey() {
		final JobGraph jobGraph = getWorkingJobGraph();
		jobGraph.addUserJarBlobKey(new PermanentBlobKey());
		return jobGraph;
	}

	// --------------------------------------------------------------------------------------------

	private static class FailingJobVertex extends JobVertex {
		private static final long serialVersionUID = -6365291240199412135L;

		public FailingJobVertex(final String msg) {
			super(msg);
		}

		@Override
		public void initializeOnMaster(ClassLoader loader) throws Exception {
			throw new Exception("Test exception.");
		}
	}
}
