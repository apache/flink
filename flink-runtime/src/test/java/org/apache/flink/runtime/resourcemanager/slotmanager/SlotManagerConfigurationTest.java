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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link SlotManagerConfiguration}.
 */
public class SlotManagerConfigurationTest extends TestLogger {

	/**
	 * Tests that {@link SlotManagerConfiguration#getSlotRequestTimeout()} returns the value
	 * configured under key {@link JobManagerOptions#SLOT_REQUEST_TIMEOUT}.
	 */
	@Test
	public void testSetSlotRequestTimeout() throws Exception {
		final long slotIdleTimeout = 42;

		final Configuration configuration = new Configuration();
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, slotIdleTimeout);
		final SlotManagerConfiguration slotManagerConfiguration = SlotManagerConfiguration.fromConfiguration(configuration);

		assertThat(slotManagerConfiguration.getSlotRequestTimeout().toMilliseconds(), is(equalTo(slotIdleTimeout)));
	}

	/**
	 * Tests that {@link ResourceManagerOptions#SLOT_REQUEST_TIMEOUT} is preferred over
	 * {@link JobManagerOptions#SLOT_REQUEST_TIMEOUT} if set.
	 */
	@Test
	public void testPreferLegacySlotRequestTimeout() throws Exception {
		final long legacySlotIdleTimeout = 42;

		final Configuration configuration = new Configuration();
		configuration.setLong(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT, legacySlotIdleTimeout);
		configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 300000L);
		final SlotManagerConfiguration slotManagerConfiguration = SlotManagerConfiguration.fromConfiguration(configuration);

		assertThat(slotManagerConfiguration.getSlotRequestTimeout().toMilliseconds(), is(equalTo(legacySlotIdleTimeout)));
	}
}
