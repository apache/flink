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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** Base class of all scheduler benchmarks. */
public class SchedulerBenchmarkBase {
    public ScheduledExecutorService scheduledExecutorService;

    public void setup() {
        // This may have been set in subclass for special purposes.
        if (scheduledExecutorService == null) {
            scheduledExecutorService =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory("flink-benchmarks"));
        }
    }

    public void teardown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
