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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.webmonitor.stats.Statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Thread info statistics of multiple tasks. Each subtask can deliver multiple samples for
 * statistical purposes.
 */
public class JobVertexThreadInfoStats implements Statistics {

    /** ID of the corresponding request. */
    private final int requestId;

    /** Timestamp, when the sample was triggered. */
    private final long startTime;

    /** Timestamp, when all samples were collected. */
    private final long endTime;

    /** Map of thread info samples by execution ID. */
    private final Map<ExecutionAttemptID, List<ThreadInfoSample>> samplesBySubtask;

    /**
     * Creates a thread details sample.
     *
     * @param requestId ID of the sample.
     * @param startTime Timestamp, when the sample was triggered.
     * @param endTime Timestamp, when all thread info samples were collected.
     * @param samplesBySubtask Map of thread info samples by subtask (execution ID).
     */
    public JobVertexThreadInfoStats(
            int requestId,
            long startTime,
            long endTime,
            Map<ExecutionAttemptID, List<ThreadInfoSample>> samplesBySubtask) {

        checkArgument(requestId >= 0, "Negative request ID");
        checkArgument(startTime >= 0, "Negative start time");
        checkArgument(endTime >= startTime, "End time before start time");

        this.requestId = requestId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.samplesBySubtask = Collections.unmodifiableMap(samplesBySubtask);
    }

    /**
     * Returns the ID of the sample.
     *
     * @return ID of the sample
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Returns the timestamp, when the sample was triggered.
     *
     * @return Timestamp, when the sample was triggered
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the timestamp, when all samples where collected.
     *
     * @return Timestamp, when all samples where collected
     */
    @Override
    public long getEndTime() {
        return endTime;
    }

    /**
     * Returns the a map of thread info samples by subtask (execution ID).
     *
     * @return Map of thread info samples by task (execution ID)
     */
    public Map<ExecutionAttemptID, List<ThreadInfoSample>> getSamplesBySubtask() {
        return samplesBySubtask;
    }

    public int getNumberOfSubtasks() {
        return samplesBySubtask.keySet().size();
    }

    @Override
    public String toString() {
        return "VertexThreadInfoStats{"
                + "sampleId="
                + requestId
                + ", startTime="
                + startTime
                + ", endTime="
                + endTime
                + '}';
    }
}
