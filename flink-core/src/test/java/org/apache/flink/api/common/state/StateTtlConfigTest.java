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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link StateTtlConfig}. */
class StateTtlConfigTest {

    @Test
    void testStateTtlConfigBuildWithoutCleanupInBackground() {
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.seconds(1)).disableCleanupInBackground().build();

        assertThat(ttlConfig.getCleanupStrategies()).isNotNull();

        CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
        IncrementalCleanupStrategy incrementalCleanupStrategy =
                cleanupStrategies.getIncrementalCleanupStrategy();
        RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
                cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

        assertThat(cleanupStrategies.isCleanupInBackground()).isFalse();
        assertThat(incrementalCleanupStrategy).isNull();
        assertThat(rocksdbCleanupStrategy).isNull();
        assertThat(cleanupStrategies.inRocksdbCompactFilter()).isFalse();
    }

    @Test
    void testStateTtlConfigBuildWithCleanupInBackground() {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(1)).build();

        assertThat(ttlConfig.getCleanupStrategies()).isNotNull();

        CleanupStrategies cleanupStrategies = ttlConfig.getCleanupStrategies();
        IncrementalCleanupStrategy incrementalCleanupStrategy =
                cleanupStrategies.getIncrementalCleanupStrategy();
        RocksdbCompactFilterCleanupStrategy rocksdbCleanupStrategy =
                cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();

        assertThat(cleanupStrategies.isCleanupInBackground()).isTrue();
        assertThat(incrementalCleanupStrategy).isNotNull();
        assertThat(rocksdbCleanupStrategy).isNotNull();
        assertThat(cleanupStrategies.inRocksdbCompactFilter()).isTrue();
        assertThat(incrementalCleanupStrategy.getCleanupSize()).isEqualTo(5);
        assertThat(incrementalCleanupStrategy.runCleanupForEveryRecord()).isFalse();
        assertThat(rocksdbCleanupStrategy.getQueryTimeAfterNumEntries()).isEqualTo(1000L);
        assertThat(rocksdbCleanupStrategy.getPeriodicCompactionTime())
                .isEqualTo(Duration.ofDays(30));
    }

    @Test
    void testStateTtlConfigBuildWithNonPositiveCleanupIncrementalSize() {
        List<Integer> illegalCleanUpSizes = Arrays.asList(0, -2);

        for (Integer illegalCleanUpSize : illegalCleanUpSizes) {
            assertThatThrownBy(
                            () ->
                                    StateTtlConfig.newBuilder(Time.seconds(1))
                                            .cleanupIncrementally(illegalCleanUpSize, false)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
