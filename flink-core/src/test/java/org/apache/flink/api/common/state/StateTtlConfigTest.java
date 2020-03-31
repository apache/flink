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

import org.apache.flink.api.common.time.Time;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link StateTtlConfig}.
 */
public class StateTtlConfigTest {

	@Test
	public void testStateTtlConfigBuildWithCleanupInBackground() {
		StateTtlConfig ttlConfig = StateTtlConfig
			.newBuilder(Time.seconds(1))
			.cleanupInBackground()
			.build();

		assertNotNull(ttlConfig.getCleanupStrategies());

		StateTtlConfig.CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
		StateTtlConfig.IncrementalCleanupStrategy incrementalCleanupStrategy =
			cleanupStrategies.getIncrementalCleanupStrategy();
		StateTtlConfig.RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
			cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

		assertTrue(cleanupStrategies.isCleanupInBackground());
		assertNotNull(incrementalCleanupStrategy);
		assertNotNull(rocksdbCleanupStrategy);
		assertTrue(cleanupStrategies.inRocksdbCompactFilter());
		assertEquals(5, incrementalCleanupStrategy.getCleanupSize());
		assertFalse(incrementalCleanupStrategy.runCleanupForEveryRecord());
		assertEquals(1000, rocksdbCleanupStrategy.getQueryTimeAfterNumEntries());
	}

}
