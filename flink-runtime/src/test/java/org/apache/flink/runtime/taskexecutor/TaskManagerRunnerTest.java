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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link TaskManagerRunner}.
 */
public class TaskManagerRunnerTest extends TestLogger {

	@Test
	public void testTaskManagerRunnerShutdown() throws Exception {
		final Configuration configuration = new Configuration();
		final ResourceID taskManagerResourceId = ResourceID.generate();

		final ServerSocket localhost = new ServerSocket(0);

		configuration.setString(JobManagerOptions.ADDRESS, localhost.getInetAddress().getHostName());
		configuration.setInteger(JobManagerOptions.PORT, localhost.getLocalPort());
		configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "10 ms");
		final CompletableFuture<Void> jvmTerminationFuture = new CompletableFuture<>();
		final TestingTaskManagerRunner taskManagerRunner = new TestingTaskManagerRunner(configuration, taskManagerResourceId, jvmTerminationFuture);

		taskManagerRunner.start();

		try {
			// wait until we trigger the jvm termination
			jvmTerminationFuture.get();

			assertThat(taskManagerRunner.getTerminationFuture().isDone(), is(true));
		} finally {
			localhost.close();
			taskManagerRunner.close();
		}
	}

	private static class TestingTaskManagerRunner extends TaskManagerRunner {

		private final CompletableFuture<Void> jvmTerminationFuture;

		public TestingTaskManagerRunner(Configuration configuration, ResourceID resourceId, CompletableFuture<Void> jvmTerminationFuture) throws Exception {
			super(configuration, resourceId);
			this.jvmTerminationFuture = jvmTerminationFuture;
		}

		@Override
		protected void terminateJVM() {
			jvmTerminationFuture.complete(null);
		}
	}
}
