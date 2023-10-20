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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.state.CheckpointExpiredThreadDumper;

import static org.apache.flink.configuration.ClusterOptions.ThreadDumpLogLevel;

/**
 * A wrapper that encapsulates the dumper worker and some job-related data and configuration,
 * convenient to request a thread dump for subtask.
 */
public class CheckpointExpiredThreadDumperWrapper {

    private final CheckpointExpiredThreadDumper dumper;

    private final JobID jobID;

    private final ThreadDumpLogLevel dumpLogLevel;

    private final int maxDepthOfThreadDump;

    CheckpointExpiredThreadDumperWrapper(
            CheckpointExpiredThreadDumper dumper,
            JobID jobID,
            ThreadDumpLogLevel dumpLogLevel,
            int maxDepthOfThreadDump) {
        this.dumper = dumper;
        this.jobID = jobID;
        this.dumpLogLevel = dumpLogLevel;
        this.maxDepthOfThreadDump = maxDepthOfThreadDump;
    }

    /**
     * Print the thread dump to log for a checkpoint. This can be ignored. See {@link
     * CheckpointExpiredThreadDumper#threadDumpIfNeeded}.
     *
     * @param checkpointId the id of aborted checkpoint.
     */
    public void threadDumpIfNeeded(long checkpointId) {
        dumper.threadDumpIfNeeded(jobID, checkpointId, dumpLogLevel, maxDepthOfThreadDump);
    }
}
