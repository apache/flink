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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import javax.annotation.Nonnegative;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Back pressure stats for one or more tasks.
 *
 * <p>The stats are collected by request triggered in {@link BackPressureRequestCoordinator}.
 */
public class BackPressureStats {

    /** ID of the request (unique per job). */
    private final int requestId;

    /** Time stamp, when the request was triggered. */
    private final long startTime;

    /**
     * Time stamp, when all back pressure stats were collected at the
     * BackPressureRequestCoordinator.
     */
    private final long endTime;

    /** Map of back pressure ratios by execution ID. */
    private final Map<ExecutionAttemptID, Double> backPressureRatios;

    public BackPressureStats(
            @Nonnegative int requestId,
            @Nonnegative long startTime,
            @Nonnegative long endTime,
            Map<ExecutionAttemptID, Double> backPressureRatios) {
        checkArgument(endTime >= startTime, "End time must not before start time.");

        this.requestId = requestId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.backPressureRatios = Collections.unmodifiableMap(checkNotNull(backPressureRatios));
    }

    public int getRequestId() {
        return requestId;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Map<ExecutionAttemptID, Double> getBackPressureRatios() {
        return backPressureRatios;
    }

    @Override
    public String toString() {
        return "BackPressureStats{"
                + "requestId="
                + requestId
                + ", startTime="
                + startTime
                + ", endTime="
                + endTime
                + '}';
    }
}
