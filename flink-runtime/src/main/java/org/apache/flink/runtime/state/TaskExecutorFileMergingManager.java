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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManagerBuilder;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingType;
import org.apache.flink.runtime.checkpoint.filemerging.PhysicalFilePool;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_ACROSS_BOUNDARY;
import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_ENABLED;
import static org.apache.flink.configuration.CheckpointingOptions.FILE_MERGING_MAX_FILE_SIZE;
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
    private final Map<JobID, FileMergingSnapshotManager> fileMergingSnapshotManagerByJobId;

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
    public @Nullable FileMergingSnapshotManager fileMergingSnapshotManagerForJob(
            @Nonnull JobID jobId,
            Configuration clusterConfiguration,
            Configuration jobConfiguration) {
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
            FileMergingSnapshotManager fileMergingSnapshotManager =
                    fileMergingSnapshotManagerByJobId.get(jobId);
            if (fileMergingSnapshotManager == null) {
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

                fileMergingSnapshotManager =
                        new FileMergingSnapshotManagerBuilder(jobId.toString(), fileMergingType)
                                .setMaxFileSize(maxFileSize.getBytes())
                                .setFilePoolType(
                                        usingBlockingPool
                                                ? PhysicalFilePool.Type.BLOCKING
                                                : PhysicalFilePool.Type.NON_BLOCKING)
                                .build();
                fileMergingSnapshotManagerByJobId.put(jobId, fileMergingSnapshotManager);
                LOG.info("Registered new file merging snapshot manager for job {}.", jobId);
            }
            return fileMergingSnapshotManager;
        }
    }

    /**
     * Release file merging snapshot manager of one job when {@link
     * org.apache.flink.runtime.taskexecutor.TaskExecutor#releaseJobResources} called.
     */
    public void releaseMergingSnapshotManagerForJob(@Nonnull JobID jobId) {
        LOG.debug("Releasing file merging snapshot manager under job id {}.", jobId);
        FileMergingSnapshotManager toRelease = null;
        synchronized (lock) {
            if (closed) {
                return;
            }
            toRelease = fileMergingSnapshotManagerByJobId.remove(jobId);
        }

        if (toRelease != null) {
            try {
                toRelease.close();
            } catch (Exception e) {
                LOG.warn(
                        "Exception while closing TaskExecutorFileMergingManager for job {}.",
                        jobId,
                        e);
            }
        }
    }

    public void shutdown() {
        HashMap<JobID, FileMergingSnapshotManager> toRelease =
                new HashMap<>(fileMergingSnapshotManagerByJobId);
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            fileMergingSnapshotManagerByJobId.clear();
        }

        LOG.info("Shutting down TaskExecutorFileMergingManager.");

        ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

        for (Map.Entry<JobID, FileMergingSnapshotManager> entry : toRelease.entrySet()) {
            if (entry.getValue() != null) {
                try {
                    entry.getValue().close();
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
