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

package org.apache.flink.runtime.testutils;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** Convenience functions to test actor based components. */
public class TestingUtils {

    public static final Duration TESTING_DURATION = Duration.ofMinutes(2L);
    public static final Time TIMEOUT = Time.minutes(1L);
    public static final Duration DEFAULT_AKKA_ASK_TIMEOUT = Duration.ofSeconds(200);

    private static ScheduledExecutorService sharedExecutorInstance;

    public static Time infiniteTime() {
        return Time.milliseconds(Integer.MAX_VALUE);
    }

    public static synchronized ScheduledExecutorService defaultExecutor() {
        if (sharedExecutorInstance == null || sharedExecutorInstance.isShutdown()) {
            sharedExecutorInstance = Executors.newSingleThreadScheduledExecutor();
        }

        return sharedExecutorInstance;
    }

    public static ScheduledExecutor defaultScheduledExecutor() {
        return new ScheduledExecutorServiceAdapter(defaultExecutor());
    }
}
