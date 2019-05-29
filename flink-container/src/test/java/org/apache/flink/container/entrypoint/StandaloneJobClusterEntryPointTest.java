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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Optional;

import static org.apache.flink.container.entrypoint.StandaloneJobClusterEntryPoint.ZERO_JOB_ID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

/**
 * Tests for the {@link StandaloneJobClusterEntryPoint}.
 */
public class StandaloneJobClusterEntryPointTest extends TestLogger {

	@Test
	public void configuredJobIDTakesPrecedenceWithHA() {
		Optional<JobID> jobID = Optional.of(JobID.generate());

		Configuration globalConfiguration = new Configuration();
		enableHighAvailability(globalConfiguration);

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobID,
			globalConfiguration);

		assertThat(jobIdForCluster, is(jobID.get()));
	}

	@Test
	public void configuredJobIDTakesPrecedenceWithoutHA() {
		Optional<JobID> jobID = Optional.of(JobID.generate());

		Configuration globalConfiguration = new Configuration();

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobID,
			globalConfiguration);

		assertThat(jobIdForCluster, is(jobID.get()));
	}

	@Test
	public void jobIDdefaultsToZeroWithHA() {
		Optional<JobID> jobID = Optional.empty();

		Configuration globalConfiguration = new Configuration();
		enableHighAvailability(globalConfiguration);

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobID,
			globalConfiguration);

		assertThat(jobIdForCluster, is(ZERO_JOB_ID));
	}

	@Test
	public void jobIDdefaultsToRandomJobIDWithoutHA() {
		Optional<JobID> jobID = Optional.empty();

		Configuration globalConfiguration = new Configuration();

		JobID jobIdForCluster = StandaloneJobClusterEntryPoint.resolveJobIdForCluster(
			jobID,
			globalConfiguration);

		assertThat(jobIdForCluster, is(not(ZERO_JOB_ID)));
	}

	private static void enableHighAvailability(final Configuration configuration) {
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
	}
}
