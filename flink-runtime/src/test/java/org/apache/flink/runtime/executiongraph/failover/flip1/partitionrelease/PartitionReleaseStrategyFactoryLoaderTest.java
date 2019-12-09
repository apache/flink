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

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;

import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link PartitionReleaseStrategyFactoryLoader}.
 */
public class PartitionReleaseStrategyFactoryLoaderTest {

	@Test
	public void featureEnabledByDefault() {
		final Configuration emptyConfiguration = new Configuration();
		final PartitionReleaseStrategy.Factory factory =
			PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(emptyConfiguration);

		assertThat(factory, is(instanceOf(RegionPartitionReleaseStrategy.Factory.class)));
	}

	@Test
	public void featureCanBeDisabled() {
		final Configuration emptyConfiguration = new Configuration();
		emptyConfiguration.setBoolean(JobManagerOptions.PARTITION_RELEASE_DURING_JOB_EXECUTION, false);

		final PartitionReleaseStrategy.Factory factory =
			PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(emptyConfiguration);

		assertThat(factory, is(instanceOf(NotReleasingPartitionReleaseStrategy.Factory.class)));
	}
}
