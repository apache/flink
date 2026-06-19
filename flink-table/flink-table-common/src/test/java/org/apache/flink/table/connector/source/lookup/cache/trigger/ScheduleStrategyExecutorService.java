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

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link ManuallyTriggeredScheduledExecutorService} that differs fixedRate and fixedDelay tasks.
 */
public class ScheduleStrategyExecutorService extends ManuallyTriggeredScheduledExecutorService {

    private int numPeriodicTasksWithFixedDelay = 0;
    private int numPeriodicTasksWithFixedRate = 0;

    public int getNumPeriodicTasksWithFixedDelay() {
        return numPeriodicTasksWithFixedDelay;
    }

    public int getNumPeriodicTasksWithFixedRate() {
        return numPeriodicTasksWithFixedRate;
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        ScheduledFuture<?> future = super.scheduleAtFixedRate(command, initialDelay, period, unit);
        numPeriodicTasksWithFixedRate++;
        return future;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        ScheduledFuture<?> future =
                super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        numPeriodicTasksWithFixedDelay++;
        return future;
    }
}
