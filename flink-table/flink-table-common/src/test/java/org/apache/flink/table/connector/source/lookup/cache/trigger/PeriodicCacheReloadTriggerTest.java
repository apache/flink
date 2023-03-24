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

package org.apache.flink.table.connector.source.lookup.cache.trigger;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_RELOAD_STRATEGY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.FULL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.PARTIAL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy.PERIODIC;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy.TIMED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link PeriodicCacheReloadTrigger}. */
class PeriodicCacheReloadTriggerTest {

    private final ScheduleStrategyExecutorService scheduledExecutor =
            new ScheduleStrategyExecutorService();

    private final TestTriggerContext context = new TestTriggerContext();

    @ParameterizedTest
    @EnumSource(ScheduleMode.class)
    void testNormalReloadInterval(ScheduleMode scheduleMode) throws Exception {
        Duration reloadInterval = Duration.ofSeconds(10);
        try (PeriodicCacheReloadTrigger trigger =
                new PeriodicCacheReloadTrigger(reloadInterval, scheduleMode, scheduledExecutor)) {
            trigger.open(context);
            checkExecutorCallByScheduleMode(reloadInterval, scheduleMode);
            scheduledExecutor.triggerPeriodicScheduledTasks();
            assertThat(context.getReloadTask().getNumLoads()).isEqualTo(1);
        }
        assertThat(scheduledExecutor.isTerminated()).isTrue();
    }

    @Test
    void testNegativeReloadInterval() {
        assertThatThrownBy(
                        () ->
                                new PeriodicCacheReloadTrigger(
                                        Duration.ZERO, ScheduleMode.FIXED_DELAY, scheduledExecutor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than zero");
        assertThatThrownBy(
                        () ->
                                new PeriodicCacheReloadTrigger(
                                        Duration.ofSeconds(-1),
                                        ScheduleMode.FIXED_DELAY,
                                        scheduledExecutor))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than zero");
    }

    @Test
    void testCreateFromConfig() {
        assertThat(PeriodicCacheReloadTrigger.fromConfig(createValidConf())).isNotNull();
        Configuration conf1 = createValidConf().set(CACHE_TYPE, PARTIAL);
        assertThatThrownBy(() -> PeriodicCacheReloadTrigger.fromConfig(conf1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be 'FULL'");
        Configuration conf2 = createValidConf().set(FULL_CACHE_RELOAD_STRATEGY, TIMED);
        assertThatThrownBy(() -> PeriodicCacheReloadTrigger.fromConfig(conf2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be 'PERIODIC'");
        Configuration conf3 = createValidConf();
        conf3.removeConfig(FULL_CACHE_PERIODIC_RELOAD_INTERVAL);
        assertThatThrownBy(() -> PeriodicCacheReloadTrigger.fromConfig(conf3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Missing '" + FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key() + "'");
    }

    private void checkExecutorCallByScheduleMode(
            Duration reloadInterval, ScheduleMode scheduleMode) {
        switch (scheduleMode) {
            case FIXED_RATE:
                assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedRate()).isEqualTo(1);
                assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedDelay()).isEqualTo(0);
                break;
            case FIXED_DELAY:
                assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedRate()).isEqualTo(0);
                assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedDelay()).isEqualTo(1);
                break;
            default:
                throw new IllegalArgumentException("Unknown schedule mode: " + scheduleMode);
        }
        Collection<ScheduledFuture<?>> tasks = scheduledExecutor.getActivePeriodicScheduledTask();
        assertThat(tasks.size()).isEqualTo(1);
        ScheduledTask<?> task = (ScheduledTask<?>) tasks.iterator().next();
        assertThat(task.getDelay(TimeUnit.MILLISECONDS)).isEqualTo(0L);
        assertThat(task.getPeriod()).isEqualTo(reloadInterval.toMillis());
    }

    private static Configuration createValidConf() {
        Configuration configuration = new Configuration();
        configuration.set(CACHE_TYPE, FULL);
        configuration.set(FULL_CACHE_RELOAD_STRATEGY, PERIODIC);
        configuration.set(FULL_CACHE_PERIODIC_RELOAD_INTERVAL, Duration.ofSeconds(1));
        return configuration;
    }
}
