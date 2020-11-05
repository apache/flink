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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;

import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link KubernetesRunningJobsRegistry} operations.
 */
public class KubernetesRunningJobsRegistryTest extends KubernetesHighAvailabilityTestBase {

	private final JobID jobID = JobID.generate();

	@Test
	public void testSetAndGetJobStatus() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesRunningJobsRegistry runningJobsRegistry = new KubernetesRunningJobsRegistry(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
					runningJobsRegistry.setJobRunning(jobID);
					assertThat(
						runningJobsRegistry.getJobSchedulingStatus(jobID),
						is(RunningJobsRegistry.JobSchedulingStatus.RUNNING));
				});
		}};
	}

	@Test
	public void testGetJobStatusNonExisting() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesRunningJobsRegistry runningJobsRegistry = new KubernetesRunningJobsRegistry(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
					final JobID jobId = JobID.generate();
					assertThat(
						runningJobsRegistry.getJobSchedulingStatus(jobId),
						is(RunningJobsRegistry.JobSchedulingStatus.PENDING));
				});
		}};
	}

	@Test
	public void testGetJobStatusConfigMapNotExist() throws Exception {
		new Context() {{
			runTest(
				() -> {
					final KubernetesRunningJobsRegistry runningJobsRegistry = new KubernetesRunningJobsRegistry(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
					try {
						runningJobsRegistry.getJobSchedulingStatus(JobID.generate());
						fail("Exception should be thrown.");
					} catch (IOException ex) {
						final String msg = "ConfigMap " + LEADER_CONFIGMAP_NAME + " does not exist";
						assertThat(ex, FlinkMatchers.containsMessage(msg));
					}
				});
		}};
	}

	@Test
	public void testClearJob() throws Exception {
		new Context() {{
			runTest(
				() -> {
					leaderCallbackGrantLeadership();

					final KubernetesRunningJobsRegistry runningJobsRegistry = new KubernetesRunningJobsRegistry(
						flinkKubeClient, LEADER_CONFIGMAP_NAME, LOCK_IDENTITY);
					runningJobsRegistry.setJobFinished(jobID);
					assertThat(
						runningJobsRegistry.getJobSchedulingStatus(jobID),
						is(RunningJobsRegistry.JobSchedulingStatus.DONE));
					runningJobsRegistry.clearJob(jobID);
					assertThat(
						runningJobsRegistry.getJobSchedulingStatus(jobID),
						is(RunningJobsRegistry.JobSchedulingStatus.PENDING));
				});
		}};
	}
}
