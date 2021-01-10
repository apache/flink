/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Samples whether a task is back pressured multi times. The total number of samples divided by the
 * number of back pressure samples reaches the back pressure ratio.
 */
public class BackPressureSampleService {

    /** Number of samples to take when determining the back pressure of a task. */
    private final int numSamples;

    /** Time to wait between samples when determining the back pressure of a task. */
    private final Time delayBetweenSamples;

    /** Executor to run back pressures sample tasks. */
    private final ScheduledExecutor scheduledExecutor;

    BackPressureSampleService(
            int numSamples, Time delayBetweenSamples, ScheduledExecutor scheduledExecutor) {

        checkArgument(numSamples >= 1, "Illegal number of samples: " + numSamples);

        this.numSamples = numSamples;
        this.delayBetweenSamples = checkNotNull(delayBetweenSamples);
        this.scheduledExecutor = checkNotNull(scheduledExecutor);
    }

    /**
     * Schedules to sample the task back pressure and returns a future that completes with the back
     * pressure ratio.
     *
     * @param task The task to be sampled.
     * @return A future containing the task back pressure ratio.
     */
    public CompletableFuture<Double> sampleTaskBackPressure(BackPressureSampleableTask task) {
        if (!task.isRunning()) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot sample task. Because the sampled task %s is not running.",
                            task));
        }

        return sampleTaskBackPressure(
                checkNotNull(task),
                numSamples,
                new ArrayList<>(numSamples),
                new CompletableFuture<>());
    }

    private CompletableFuture<Double> sampleTaskBackPressure(
            BackPressureSampleableTask task,
            int remainingNumSamples,
            List<Boolean> taskBackPressureSamples,
            CompletableFuture<Double> resultFuture) {
        taskBackPressureSamples.add(task.isBackPressured());

        if (task.isRunning() && remainingNumSamples > 1) {
            scheduledExecutor.schedule(
                    () ->
                            sampleTaskBackPressure(
                                    task,
                                    remainingNumSamples - 1,
                                    taskBackPressureSamples,
                                    resultFuture),
                    delayBetweenSamples.getSize(),
                    delayBetweenSamples.getUnit());
        } else {
            resultFuture.complete(calculateTaskBackPressureRatio(taskBackPressureSamples));
        }

        return resultFuture;
    }

    private double calculateTaskBackPressureRatio(final List<Boolean> taskBackPressureSamples) {
        double backPressureCount = 0.0;
        for (Boolean isBackPressured : taskBackPressureSamples) {
            if (isBackPressured) {
                ++backPressureCount;
            }
        }
        return taskBackPressureSamples.isEmpty()
                ? 0.0
                : backPressureCount / taskBackPressureSamples.size();
    }
}
