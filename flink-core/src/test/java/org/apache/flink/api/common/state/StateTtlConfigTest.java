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

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Fail.fail;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for the {@link StateTtlConfig}. */
class StateTtlConfigTest {

    @Test
    void testStateTtlConfigBuildWithoutCleanupInBackground() {
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.seconds(1)).disableCleanupInBackground().build();

        MatcherAssert.assertThat(ttlConfig.getCleanupStrategies(), notNullValue());

        CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
        IncrementalCleanupStrategy incrementalCleanupStrategy =
                cleanupStrategies.getIncrementalCleanupStrategy();
        RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
                cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

        MatcherAssert.assertThat(cleanupStrategies.isCleanupInBackground(), is(false));
        MatcherAssert.assertThat(incrementalCleanupStrategy, nullValue());
        MatcherAssert.assertThat(rocksdbCleanupStrategy, nullValue());
        MatcherAssert.assertThat(cleanupStrategies.inRocksdbCompactFilter(), is(false));
    }

    @Test
    void testStateTtlConfigBuildWithCleanupInBackground() {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(1)).build();

        MatcherAssert.assertThat(ttlConfig.getCleanupStrategies(), notNullValue());

        CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
        IncrementalCleanupStrategy incrementalCleanupStrategy =
                cleanupStrategies.getIncrementalCleanupStrategy();
        RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
                cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

        MatcherAssert.assertThat(cleanupStrategies.isCleanupInBackground(), is(true));
        MatcherAssert.assertThat(incrementalCleanupStrategy, notNullValue());
        MatcherAssert.assertThat(rocksdbCleanupStrategy, notNullValue());
        MatcherAssert.assertThat(cleanupStrategies.inRocksdbCompactFilter(), is(true));
        MatcherAssert.assertThat(incrementalCleanupStrategy.getCleanupSize(), is(5));
        MatcherAssert.assertThat(incrementalCleanupStrategy.runCleanupForEveryRecord(), is(false));
        MatcherAssert.assertThat(rocksdbCleanupStrategy.getQueryTimeAfterNumEntries(), is(1000L));
        MatcherAssert.assertThat(
                rocksdbCleanupStrategy.getPeriodicCompactionTime(), is(Time.days(30)));
    }

    @Test
    void testStateTtlConfigBuildWithNonPositiveCleanupIncrementalSize() {
        List<Integer> illegalCleanUpSizes = Arrays.asList(0, -2);

        for (Integer illegalCleanUpSize : illegalCleanUpSizes) {
            try {
                StateTtlConfig ttlConfig =
                        StateTtlConfig.newBuilder(Time.seconds(1))
                                .cleanupIncrementally(illegalCleanUpSize, false)
                                .build();
                fail();
            } catch (IllegalArgumentException e) {
            }
        }
    }
}
