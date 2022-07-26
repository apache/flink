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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.time.Instant.ofEpochMilli;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_RELOAD_STRATEGY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.FULL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.PARTIAL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy.PERIODIC;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy.TIMED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link TimedCacheReloadTrigger}. */
class TimedCacheReloadTriggerTest {

    private static final long MILLIS_DIFF = 2L;
    private static final int RELOAD_INTERVAL_IN_DAYS = 7;
    private static final Clock CONSTANT_CLOCK =
            Clock.fixed(ofEpochMilli(10), ZoneId.systemDefault());

    private final ScheduleStrategyExecutorService scheduledExecutor =
            new ScheduleStrategyExecutorService();

    private final TestTriggerContext context = new TestTriggerContext();

    @ParameterizedTest
    @MethodSource("normalReloadTimes")
    void testNormalReloadTime(Temporal reloadTime) throws Exception {
        try (TimedCacheReloadTrigger trigger =
                new TimedCacheReloadTrigger(
                        reloadTime, RELOAD_INTERVAL_IN_DAYS, scheduledExecutor, CONSTANT_CLOCK)) {
            trigger.open(context);
            assertThat(scheduledExecutor.numQueuedRunnables()).isEqualTo(1);

            assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedDelay()).isEqualTo(0);
            assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedRate()).isEqualTo(1);
            ScheduledTask<?> scheduledLoadTask =
                    (ScheduledTask<?>)
                            scheduledExecutor.getAllPeriodicScheduledTask().iterator().next();
            assertThat(scheduledLoadTask.getDelay(TimeUnit.MILLISECONDS)).isEqualTo(MILLIS_DIFF);
            assertThat(scheduledLoadTask.getPeriod())
                    .isEqualTo(Duration.ofDays(RELOAD_INTERVAL_IN_DAYS).toMillis());

            scheduledExecutor.trigger();
            assertThat(context.getReloadTask().getNumLoads()).isEqualTo(1);
            scheduledExecutor.triggerPeriodicScheduledTasks();
            assertThat(context.getReloadTask().getNumLoads()).isEqualTo(2);
        }
        assertThat(scheduledExecutor.isTerminated()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("nextDayReloadTimes")
    void testNextDayReloadTime(Temporal reloadTime) throws Exception {
        try (TimedCacheReloadTrigger trigger =
                new TimedCacheReloadTrigger(
                        reloadTime, RELOAD_INTERVAL_IN_DAYS, scheduledExecutor, CONSTANT_CLOCK)) {
            trigger.open(context);
            assertThat(scheduledExecutor.numQueuedRunnables()).isEqualTo(1);

            assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedDelay()).isEqualTo(0);
            assertThat(scheduledExecutor.getNumPeriodicTasksWithFixedRate()).isEqualTo(1);
            ScheduledTask<?> scheduledLoadTask =
                    (ScheduledTask<?>)
                            scheduledExecutor.getAllPeriodicScheduledTask().iterator().next();
            assertThat(scheduledLoadTask.getDelay(TimeUnit.MILLISECONDS))
                    .isEqualTo(Duration.ofDays(1).minus(MILLIS_DIFF, ChronoUnit.MILLIS).toMillis());
            assertThat(scheduledLoadTask.getPeriod())
                    .isEqualTo(Duration.ofDays(RELOAD_INTERVAL_IN_DAYS).toMillis());
            scheduledExecutor.trigger();
            assertThat(context.getReloadTask().getNumLoads()).isEqualTo(1);
            scheduledExecutor.triggerPeriodicScheduledTasks();
            assertThat(context.getReloadTask().getNumLoads()).isEqualTo(2);
        }
        assertThat(scheduledExecutor.isTerminated()).isTrue();
    }

    @Test
    void testBadReloadIntervalInDays() {
        assertThatThrownBy(() -> new TimedCacheReloadTrigger(LocalTime.now(), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 day");
        assertThatThrownBy(() -> new TimedCacheReloadTrigger(LocalTime.now(), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1 day");
    }

    @ParameterizedTest
    @MethodSource("reloadTimeStrings")
    void testParseReloadTimeFromConf(String reloadTimeStr, Temporal reloadTime) {
        Configuration configuration = createValidConf();
        configuration.set(FULL_CACHE_TIMED_RELOAD_ISO_TIME, reloadTimeStr);
        TimedCacheReloadTrigger trigger = TimedCacheReloadTrigger.fromConfig(configuration);
        assertThat(trigger.getReloadTime()).isEqualTo(reloadTime);
    }

    @Test
    void testCreateFromConfig() {
        assertThat(TimedCacheReloadTrigger.fromConfig(createValidConf())).isNotNull();
        Configuration conf1 = createValidConf().set(CACHE_TYPE, PARTIAL);
        assertThatThrownBy(() -> TimedCacheReloadTrigger.fromConfig(conf1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be 'FULL'");
        Configuration conf2 = createValidConf().set(FULL_CACHE_RELOAD_STRATEGY, PERIODIC);
        assertThatThrownBy(() -> TimedCacheReloadTrigger.fromConfig(conf2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be 'TIMED'");
        Configuration conf3 = createValidConf();
        conf3.removeConfig(FULL_CACHE_TIMED_RELOAD_ISO_TIME);
        assertThatThrownBy(() -> TimedCacheReloadTrigger.fromConfig(conf3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Missing '" + FULL_CACHE_TIMED_RELOAD_ISO_TIME.key() + "'");
        Configuration conf4 = createValidConf().set(FULL_CACHE_TIMED_RELOAD_ISO_TIME, "10");
        assertThatThrownBy(() -> TimedCacheReloadTrigger.fromConfig(conf4))
                .isInstanceOf(DateTimeParseException.class)
                .hasMessageContaining("could not be parsed");
    }

    static Stream<Arguments> normalReloadTimes() {
        return Stream.of(
                Arguments.of(OffsetTime.now(CONSTANT_CLOCK).plus(MILLIS_DIFF, ChronoUnit.MILLIS)),
                Arguments.of(LocalTime.now(CONSTANT_CLOCK).plus(MILLIS_DIFF, ChronoUnit.MILLIS)));
    }

    static Stream<Arguments> nextDayReloadTimes() {
        return Stream.of(
                Arguments.of(OffsetTime.now(CONSTANT_CLOCK).minus(MILLIS_DIFF, ChronoUnit.MILLIS)),
                Arguments.of(LocalTime.now(CONSTANT_CLOCK).minus(MILLIS_DIFF, ChronoUnit.MILLIS)));
    }

    static Stream<Arguments> reloadTimeStrings() {
        LocalTime localTime = LocalTime.of(10, 15);
        return Stream.of(
                Arguments.of("10:15", localTime),
                Arguments.of("10:15Z", OffsetTime.of(localTime, ZoneOffset.UTC)),
                Arguments.of("10:15+07:00", OffsetTime.of(localTime, ZoneOffset.ofHours(7))));
    }

    private static Configuration createValidConf() {
        Configuration configuration = new Configuration();
        configuration.set(CACHE_TYPE, FULL);
        configuration.set(FULL_CACHE_RELOAD_STRATEGY, TIMED);
        configuration.set(FULL_CACHE_TIMED_RELOAD_ISO_TIME, "10:15");
        return configuration;
    }
}
