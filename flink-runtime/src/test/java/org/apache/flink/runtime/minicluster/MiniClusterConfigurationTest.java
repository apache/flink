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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link MiniClusterConfiguration}.
 */
public class MiniClusterConfigurationTest extends TestLogger {

	private static final String TEST_SCHEDULER_NAME = "test-scheduler";

	private static String priorSchedulerType;

	@BeforeClass
	public static void setUp() {
		priorSchedulerType = System.getProperty(MiniClusterConfiguration.SCHEDULER_TYPE_KEY);
		System.setProperty(MiniClusterConfiguration.SCHEDULER_TYPE_KEY, TEST_SCHEDULER_NAME);
	}

	@AfterClass
	public static void tearDown() {
		if (priorSchedulerType != null) {
			System.setProperty(MiniClusterConfiguration.SCHEDULER_TYPE_KEY, priorSchedulerType);
			priorSchedulerType = null;
		} else {
			System.clearProperty(MiniClusterConfiguration.SCHEDULER_TYPE_KEY);
		}
	}

	@Test
	public void testSchedulerType_setViaSystemProperty() {
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder().build();

		Assert.assertEquals(
			TEST_SCHEDULER_NAME,
			miniClusterConfiguration.getConfiguration().getString(JobManagerOptions.SCHEDULER));
	}

	@Test
	public void testSchedulerType_notOverriddenIfExistingInConfig() {
		final Configuration config = new Configuration();
		config.setString(JobManagerOptions.SCHEDULER, JobManagerOptions.SCHEDULER.defaultValue());

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.build();

		Assert.assertEquals(
			JobManagerOptions.SCHEDULER.defaultValue(),
			miniClusterConfiguration.getConfiguration().getString(JobManagerOptions.SCHEDULER));
	}

	@Test
	public void testDefaultTaskExecutorMemoryConfiguration() {
		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder().build();
		final Configuration actualConfiguration = miniClusterConfiguration.getConfiguration();
		final Configuration expectedConfiguration = MiniClusterConfiguration.adjustTaskManagerMemoryConfigurations(new Configuration());

		assertThat(actualConfiguration.get(TaskManagerOptions.SHUFFLE_MEMORY_MIN), is(MiniClusterConfiguration.DEFAULT_SHUFFLE_MEMORY_SIZE));
		assertThat(actualConfiguration.get(TaskManagerOptions.SHUFFLE_MEMORY_MAX), is(MiniClusterConfiguration.DEFAULT_SHUFFLE_MEMORY_SIZE));
		assertThat(actualConfiguration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE), is(MiniClusterConfiguration.DEFAULT_MANAGED_MEMORY_SIZE));
	}
}
