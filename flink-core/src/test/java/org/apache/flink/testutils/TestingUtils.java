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

package org.apache.flink.testutils;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.executor.TestExecutorResource;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** Convenience functions to test actor based components. */
public class TestingUtils {
    private static final UUID ZERO_UUID = new UUID(0L, 0L);

    public static final Duration TESTING_DURATION = Duration.ofMinutes(2L);
    public static final Time TIMEOUT = Time.minutes(1L);
    public static final Duration DEFAULT_ASK_TIMEOUT = Duration.ofSeconds(200);

    public static Time infiniteTime() {
        return Time.milliseconds(Integer.MAX_VALUE);
    }

    public static Duration infiniteDuration() {
        // we cannot use Long.MAX_VALUE because the Duration stores it in nanosecond resolution and
        // calculations will easily cause overflows --> 1 year should be long enough for "infinity"
        return Duration.ofDays(365L);
    }

    public static TestExecutorExtension<ScheduledExecutorService> defaultExecutorExtension() {
        return new TestExecutorExtension<>(Executors::newSingleThreadScheduledExecutor);
    }

    public static TestExecutorResource<ScheduledExecutorService> defaultExecutorResource() {
        return new TestExecutorResource<>(Executors::newSingleThreadScheduledExecutor);
    }

    public static UUID zeroUUID() {
        return ZERO_UUID;
    }
}
