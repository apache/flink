/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.util.JvmUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.apache.flink.configuration.ClusterOptions.ThreadDumpLogLevel;

/**
 * A worker that can print thread dump to log when checkpoint aborted. There is a throttling
 * strategy that it can only output once for each job and each checkpoint abortion.
 */
@ThreadSafe
public class CheckpointExpiredThreadDumper {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointExpiredThreadDumper.class);

    /** A map records the last dumped checkpoint for each job. */
    private final ConcurrentHashMap<JobID, Long> jobIdLastDumpCheckpointIdMap;

    public CheckpointExpiredThreadDumper() {
        this.jobIdLastDumpCheckpointIdMap = new ConcurrentHashMap<>();
    }

    /**
     * Print the thread dump to log if needed. A thread dump is captured and print only when:
     *
     * <p>1. It is the first time to request a thread dump for this checkpoint within current job.
     *
     * <p>2. The configured log level is enabled.
     *
     * @param jobId id of the current job.
     * @param checkpointId id of the aborted checkpoint.
     * @param threadDumpLogLevelWhenAbort the configured log level for thread dump.
     * @param maxDepthOfThreadDump the max stack depth for thread dump.
     * @return true if really print the log.
     */
    public boolean threadDumpIfNeeded(
            JobID jobId,
            long checkpointId,
            ThreadDumpLogLevel threadDumpLogLevelWhenAbort,
            int maxDepthOfThreadDump) {
        AtomicBoolean needDump = new AtomicBoolean(false);
        jobIdLastDumpCheckpointIdMap.compute(
                jobId,
                (k, v) -> {
                    long lastCheckpointId = v == null ? -1L : v;
                    needDump.set(lastCheckpointId < checkpointId);
                    return Math.max(lastCheckpointId, checkpointId);
                });
        if (!needDump.get()) {
            return false;
        }
        Consumer<String> logger = null;
        switch (threadDumpLogLevelWhenAbort) {
            case TRACE:
                if (LOG.isTraceEnabled()) {
                    logger = LOG::trace;
                }
                break;
            case DEBUG:
                if (LOG.isDebugEnabled()) {
                    logger = LOG::debug;
                }
                break;
            case INFO:
                if (LOG.isInfoEnabled()) {
                    logger = LOG::info;
                }
                break;
            case WARN:
                if (LOG.isWarnEnabled()) {
                    logger = LOG::warn;
                }
                break;
            case ERROR:
                if (LOG.isErrorEnabled()) {
                    logger = LOG::error;
                }
                break;
            default:
                break;
        }
        if (logger != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Checkpoint ")
                    .append(checkpointId)
                    .append(" of job ")
                    .append(jobId.toString())
                    .append(" is notified expired, producing the thread dump for debugging:\n");
            JvmUtils.createThreadDump()
                    .forEach(
                            threadInfo ->
                                    sb.append(
                                            JvmUtils.stringifyThreadInfo(
                                                    threadInfo, maxDepthOfThreadDump)));
            logger.accept(sb.toString());
            return true;
        }
        return false;
    }

    /**
     * Remove the thread dump history for one job.
     *
     * @param jobId the specified job to remove history.
     */
    public void removeCheckpointExpiredThreadDumpRecordForJob(JobID jobId) {
        jobIdLastDumpCheckpointIdMap.remove(jobId);
    }
}
