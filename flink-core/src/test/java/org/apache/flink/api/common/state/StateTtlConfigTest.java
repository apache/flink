/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.state.StateTtlConfig.CleanupStrategies;
import org.apache.flink.api.common.state.StateTtlConfig.IncrementalCleanupStrategy;
import org.apache.flink.api.common.state.StateTtlConfig.RocksdbCompactFilterCleanupStrategy;
import org.apache.flink.api.common.time.Time;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link StateTtlConfig}.
 */
public class StateTtlConfigTest {

	@Test
	public void testStateTtlConfigBuildWithoutCleanupInBackground() {
		StateTtlConfig ttlConfig = StateTtlConfig
			.newBuilder(Time.seconds(1))
			.disableCleanupInBackground()
			.build();

		assertThat(ttlConfig.getCleanupStrategies(), notNullValue());

		CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
		IncrementalCleanupStrategy incrementalCleanupStrategy =
			cleanupStrategies.getIncrementalCleanupStrategy();
		RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
			cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

		assertThat(cleanupStrategies.isCleanupInBackground(), is(false));
		assertThat(incrementalCleanupStrategy, nullValue());
		assertThat(rocksdbCleanupStrategy, nullValue());
		assertThat(cleanupStrategies.inRocksdbCompactFilter(), is(false));
	}

	@Test
	public void testStateTtlConfigBuildWithCleanupInBackground() {
		StateTtlConfig ttlConfig = StateTtlConfig
			.newBuilder(Time.seconds(1))
			.cleanupInBackground()
			.build();

		assertThat(ttlConfig.getCleanupStrategies(), notNullValue());

		CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
		IncrementalCleanupStrategy incrementalCleanupStrategy =
			cleanupStrategies.getIncrementalCleanupStrategy();
		RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
			cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

		assertThat(cleanupStrategies.isCleanupInBackground(), is(true));
		assertThat(incrementalCleanupStrategy, notNullValue());
		assertThat(rocksdbCleanupStrategy, notNullValue());
		assertThat(cleanupStrategies.inRocksdbCompactFilter(), is(true));
		assertThat(incrementalCleanupStrategy.getCleanupSize(), is(5));
		assertThat(incrementalCleanupStrategy.runCleanupForEveryRecord(), is(false));
		assertThat(rocksdbCleanupStrategy.getQueryTimeAfterNumEntries(), is(1000L));
	}

}
