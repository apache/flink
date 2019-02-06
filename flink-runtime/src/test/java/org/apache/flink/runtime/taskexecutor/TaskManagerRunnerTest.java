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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.testutils.SecurityManagerContext;
import org.apache.flink.runtime.testutils.SystemExitTrackingSecurityManager;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link TaskManagerRunner}.
 */
public class TaskManagerRunnerTest extends TestLogger {

	@Rule
	public final Timeout timeout = Timeout.seconds(30);

	@Test
	public void testShouldShutdownOnFatalError() throws Exception {
		try (TaskManagerRunner taskManagerRunner = createTaskManagerRunner(createConfiguration())) {
			taskManagerRunner.start();

			final SystemExitTrackingSecurityManager systemExitTrackingSecurityManager =
				runWithSystemExitTracking(() -> {
					taskManagerRunner.onFatalError(new RuntimeException());
					taskManagerRunner.getTerminationFuture().get(30, TimeUnit.SECONDS);
				});

			assertThat(systemExitTrackingSecurityManager.getCount(), is(equalTo(1)));
			assertThat(systemExitTrackingSecurityManager.getStatus(), is(equalTo(TaskManagerRunner.RUNTIME_FAILURE_RETURN_CODE)));
		}
	}

	@Test
	public void testShouldShutdownIfRegistrationWithJobManagerFails() throws Exception {
		final Configuration configuration = createConfiguration();
		configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "10 ms");

		try (TaskManagerRunner taskManagerRunner = createTaskManagerRunner(configuration)) {

			final SystemExitTrackingSecurityManager systemExitTrackingSecurityManager =
				runWithSystemExitTracking(() -> {
					taskManagerRunner.start();
					taskManagerRunner.getTerminationFuture().get();
				});

			assertThat(systemExitTrackingSecurityManager.getCount(), is(equalTo(1)));
			assertThat(systemExitTrackingSecurityManager.getStatus(), is(equalTo(TaskManagerRunner.RUNTIME_FAILURE_RETURN_CODE)));
		}
	}

	private static SystemExitTrackingSecurityManager runWithSystemExitTracking(final RunnableWithException runnable) {
		final SystemExitTrackingSecurityManager systemExitTrackingSecurityManager = new SystemExitTrackingSecurityManager();
		SecurityManagerContext.runWithSecurityManager(systemExitTrackingSecurityManager, runnable);
		return systemExitTrackingSecurityManager;
	}

	private static Configuration createConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		configuration.setString(TaskManagerOptions.HOST, "localhost");
		return configuration;
	}

	private static TaskManagerRunner createTaskManagerRunner(final Configuration configuration) throws Exception {
		return new TaskManagerRunner(configuration, ResourceID.generate());
	}
}
