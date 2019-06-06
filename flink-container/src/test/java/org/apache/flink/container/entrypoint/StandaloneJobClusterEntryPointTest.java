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

package org.apache.flink.container.entrypoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint.ExecutionMode;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.flink.container.entrypoint.StandaloneJobClusterEntryPoint.ZERO_JOB_ID;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

/**
 * Tests for the {@link StandaloneJobClusterEntryPoint}.
 */
public class StandaloneJobClusterEntryPointTest extends TestLogger {

	/**
	 * Tests that the default {@link ExecutionMode} is {@link ExecutionMode#DETACHED}.
	 */
	@Test
	public void testDefaultExecutionModeIsDetached() {
		Configuration configuration = new Configuration();

		StandaloneJobClusterEntryPoint.setDefaultExecutionModeIfNotConfigured(configuration);

		assertThat(getExecutionMode(configuration), equalTo(ExecutionMode.DETACHED));
	}

	/**
	 * Tests that {@link ExecutionMode} is not overwritten if provided.
	 */
	@Test
	public void testDontOverwriteExecutionMode() {
		Configuration configuration = new Configuration();
		setExecutionMode(configuration, ExecutionMode.NORMAL);

		StandaloneJobClusterEntryPoint.setDefaultExecutionModeIfNotConfigured(configuration);

		// Don't overwrite provided configuration
		assertThat(getExecutionMode(configuration), equalTo(ExecutionMode.NORMAL));
	}

	@Test
	public void configuredJobIdTakesPrecedenceWithHA() {
		JobID jobId = JobID.generate();
		String jobIdSeed = null;

		Configuration globalConfiguration = new Configuration();
		enableHighAvailability(globalConfiguration);

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(jobId));
	}

	@Test
	public void configuredJobIdTakesPrecedenceWithoutHA() {
		JobID jobId = JobID.generate();
		String jobIdSeed = null;

		Configuration globalConfiguration = new Configuration();

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(jobId));
	}

	@Test
	public void configuredJobIdSeedTakesPrecedenceWithoutHA() {
		JobID jobId = null;
		String jobIdSeed = "some-seed";

		Configuration globalConfiguration = new Configuration();

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(StandaloneJobClusterEntryPoint.createJobIdFromSeed(jobIdSeed)));
	}

	@Test
	public void configuredJobIdSeedTakesPrecedenceWithHA() {
		JobID jobId = null;
		String jobIdSeed = "some-seed";

		Configuration globalConfiguration = new Configuration();
		enableHighAvailability(globalConfiguration);

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(StandaloneJobClusterEntryPoint.createJobIdFromSeed(jobIdSeed)));
	}

	@Test
	public void configuredJobIdTakesPrecedenceOverJobIdSeed() {
		JobID jobId = JobID.generate();
		String jobIdSeed = "some-seed";

		Configuration globalConfiguration = new Configuration();
		enableHighAvailability(globalConfiguration);

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(jobId));
	}

	@Test
	public void jobIdDefaultsToZeroWithHA() {
		JobID jobId = null;
		String jobIdSeed = null;

		Configuration globalConfiguration = new Configuration();
		enableHighAvailability(globalConfiguration);

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(ZERO_JOB_ID));
	}

	@Test
	public void jobIdDefaultsToRandomJobIdWithoutHA() {
		JobID jobId = null;
		String jobIdSeed = null;

		Configuration globalConfiguration = new Configuration();

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobId,
			jobIdSeed,
			globalConfiguration);

		assertThat(jobIdForCluster, is(not(ZERO_JOB_ID)));
	}

	@Test
	public void jobIdsFromSeedAreStable() {
		JobID jobId1 = StandaloneJobClusterEntryPoint.createJobIdFromSeed("a-great-seed");
		JobID jobId2 = StandaloneJobClusterEntryPoint.createJobIdFromSeed("another-great-seed");

		Assert.assertThat(jobId1, Matchers.is(JobID.fromHexString("3dbdcd612b64005c458307356c78cd6e")));
		Assert.assertThat(jobId2, Matchers.is(JobID.fromHexString("036e277424b92cb9920e39b9eef5d453")));
	}

	@Test
	public void jobIdsFromDifferentSeedsDiffer() {
		JobID jobId1 = StandaloneJobClusterEntryPoint.createJobIdFromSeed("some-seed");
		JobID jobId2 = StandaloneJobClusterEntryPoint.createJobIdFromSeed("some-other-seed");

		Assert.assertThat(jobId1, Matchers.is(not(jobId2)));
	}

	@Test
	public void jobIdFromSameSeedAreTheSame() {
		JobID jobId1 = StandaloneJobClusterEntryPoint.createJobIdFromSeed("same-seed");
		JobID jobId2 = StandaloneJobClusterEntryPoint.createJobIdFromSeed("same-seed");

		Assert.assertThat(jobId1, Matchers.is(jobId2));
	}

	private static void setExecutionMode(Configuration configuration, ExecutionMode executionMode) {
		configuration.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());
	}

	private static ExecutionMode getExecutionMode(Configuration configuration) {
		return ExecutionMode.valueOf(configuration.getString(ClusterEntrypoint.EXECUTION_MODE));
	}

	private static void enableHighAvailability(final Configuration configuration) {
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
	}
}
