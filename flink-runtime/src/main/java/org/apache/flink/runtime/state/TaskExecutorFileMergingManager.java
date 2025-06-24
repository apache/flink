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
package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManagerBuilder;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingType;
import org.apache.flink.runtime.checkpoint.filemerging.PhysicalFilePool;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_ACROSS_BOUNDARY;
import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_ENABLED;
import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_MAX_FILE_SIZE;
import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_MAX_SPACE_AMPLIFICATION;
import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_POOL_BLOCKING;

/**
 * There is one {@link FileMergingSnapshotManager} for each job per task manager. This class holds
 * all {@link FileMergingSnapshotManager} objects for a task executor (manager).
 */
public class TaskExecutorFileMergingManager {
    /** Logger for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorFileMergingManager.class);

    /**
     * This map holds all FileMergingSnapshotManager for tasks running on this task
     * manager(executor).
     */
    @GuardedBy("lock")
    private final Map<JobID, Tuple2<FileMergingSnapshotManager, Set<ExecutionAttemptID>>>
            fileMergingSnapshotManagerByJobId;

    @GuardedBy("lock")
    private boolean closed;

    private final Object lock = new Object();

    /** Shutdown hook for this manager. */
    private final Thread shutdownHook;

    public TaskExecutorFileMergingManager() {
        this.fileMergingSnapshotManagerByJobId = new HashMap<>();
        this.closed = false;
        this.shutdownHook =
                ShutdownHookUtil.addShutdownHook(this::shutdown, getClass().getSimpleName(), LOG);
    }

    /**
     * Initialize file merging snapshot manager for each job according configurations when {@link
     * org.apache.flink.runtime.taskexecutor.TaskExecutor#submitTask}.
     */
    public @Nullable FileMergingSnapshotManager fileMergingSnapshotManagerForTask(
            @Nonnull JobID jobId,
            @Nonnull ResourceID tmResourceId,
            @Nonnull ExecutionAttemptID executionAttemptID,
            Configuration clusterConfiguration,
            Configuration jobConfiguration,
            TaskManagerJobMetricGroup metricGroup) {
        boolean mergingEnabled =
                jobConfiguration
                        .getOptional(FILE_MERGING_ENABLED)
                        .orElse(clusterConfiguration.get(FILE_MERGING_ENABLED));
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException(
                        "TaskExecutorFileMergingManager is already closed and cannot "
                                + "register a new FileMergingSnapshotManager.");
            }
            if (!mergingEnabled) {
                return null;
            }
            Tuple2<FileMergingSnapshotManager, Set<ExecutionAttemptID>>
                    fileMergingSnapshotManagerAndRetainedExecutions =
                            fileMergingSnapshotManagerByJobId.get(jobId);
            if (fileMergingSnapshotManagerAndRetainedExecutions == null) {
                FileMergingType fileMergingType =
                        jobConfiguration
                                        .getOptional(FILE_MERGING_ACROSS_BOUNDARY)
                                        .orElse(
                                                clusterConfiguration.get(
                                                        FILE_MERGING_ACROSS_BOUNDARY))
                                ? FileMergingType.MERGE_ACROSS_CHECKPOINT
                                : FileMergingType.MERGE_WITHIN_CHECKPOINT;
                MemorySize maxFileSize =
                        jobConfiguration
                                .getOptional(FILE_MERGING_MAX_FILE_SIZE)
                                .orElse(clusterConfiguration.get(FILE_MERGING_MAX_FILE_SIZE));
                Boolean usingBlockingPool =
                        jobConfiguration
                                .getOptional(FILE_MERGING_POOL_BLOCKING)
                                .orElse(clusterConfiguration.get(FILE_MERGING_POOL_BLOCKING));

                Float spaceAmplification =
                        jobConfiguration
                                .getOptional(FILE_MERGING_MAX_SPACE_AMPLIFICATION)
                                .orElse(
                                        clusterConfiguration.get(
                                                FILE_MERGING_MAX_SPACE_AMPLIFICATION));

                fileMergingSnapshotManagerAndRetainedExecutions =
                        Tuple2.of(
                                new FileMergingSnapshotManagerBuilder(
                                                jobId, tmResourceId, fileMergingType)
                                        .setMaxFileSize(maxFileSize.getBytes())
                                        .setFilePoolType(
                                                usingBlockingPool
                                                        ? PhysicalFilePool.Type.BLOCKING
                                                        : PhysicalFilePool.Type.NON_BLOCKING)
                                        .setMaxSpaceAmplification(spaceAmplification)
                                        .setMetricGroup(metricGroup)
                                        .build(),
                                new HashSet<>());
                fileMergingSnapshotManagerByJobId.put(
                        jobId, fileMergingSnapshotManagerAndRetainedExecutions);
                LOG.info("Registered new file merging snapshot manager for job {}.", jobId);
            }
            fileMergingSnapshotManagerAndRetainedExecutions.f1.add(executionAttemptID);
            return fileMergingSnapshotManagerAndRetainedExecutions.f0;
        }
    }

    public void releaseMergingSnapshotManagerForTask(
            @Nonnull JobID jobId, @Nonnull ExecutionAttemptID executionAttemptID) {
        synchronized (lock) {
            Tuple2<FileMergingSnapshotManager, Set<ExecutionAttemptID>>
                    fileMergingSnapshotManagerAndRetainedExecutions =
                            fileMergingSnapshotManagerByJobId.get(jobId);
            if (fileMergingSnapshotManagerAndRetainedExecutions != null) {
                LOG.debug(
                        "Releasing file merging snapshot manager under job id {} and attempt {}.",
                        jobId,
                        executionAttemptID);
                fileMergingSnapshotManagerAndRetainedExecutions.f1.remove(executionAttemptID);
                if (fileMergingSnapshotManagerAndRetainedExecutions.f1.isEmpty()) {
                    releaseMergingSnapshotManagerForJob(jobId);
                }
            }
        }
    }

    /**
     * Release file merging snapshot manager of one job when {@code
     * org.apache.flink.runtime.taskexecutor.TaskExecutor#releaseJobResources} called.
     */
    public void releaseMergingSnapshotManagerForJob(@Nonnull JobID jobId) {
        LOG.debug("Releasing file merging snapshot manager under job id {}.", jobId);
        Tuple2<FileMergingSnapshotManager, Set<ExecutionAttemptID>> toRelease = null;
        synchronized (lock) {
            if (closed) {
                return;
            }
            toRelease = fileMergingSnapshotManagerByJobId.remove(jobId);
        }

        if (toRelease != null) {
            if (!toRelease.f1.isEmpty()) {
                LOG.warn(
                        "The file merging snapshot manager for job {} is released before all tasks are released.",
                        jobId);
            }
            try {
                toRelease.f0.close();
            } catch (Exception e) {
                LOG.warn(
                        "Exception while closing TaskExecutorFileMergingManager for job {}.",
                        jobId,
                        e);
            }
        }
    }

    public void shutdown() {
        Map<JobID, Tuple2<FileMergingSnapshotManager, Set<ExecutionAttemptID>>> toRelease = null;
        synchronized (lock) {
            toRelease = new HashMap<>(fileMergingSnapshotManagerByJobId);
            if (closed) {
                return;
            }
            closed = true;
            fileMergingSnapshotManagerByJobId.clear();
        }

        LOG.info("Shutting down TaskExecutorFileMergingManager.");

        ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

        for (Map.Entry<JobID, Tuple2<FileMergingSnapshotManager, Set<ExecutionAttemptID>>> entry :
                toRelease.entrySet()) {
            if (entry.getValue() != null) {
                try {
                    entry.getValue().f0.close();
                } catch (Exception e) {
                    LOG.warn(
                            "Exception while closing TaskExecutorFileMergingManager for job {}.",
                            entry.getKey(),
                            e);
                }
            }
        }
    }
}
