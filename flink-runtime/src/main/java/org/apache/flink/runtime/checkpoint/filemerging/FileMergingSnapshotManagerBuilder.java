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

package org.apache.flink.runtime.checkpoint.filemerging;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;

/** A builder that builds the {@link FileMergingSnapshotManager}. */
public class FileMergingSnapshotManagerBuilder {

    // Id format for FileMergingSnapshotManager, consist with jobId and tmId
    private static final String ID_FORMAT = "job_%s_tm_%s";

    private final JobID jobId;

    private final ResourceID tmResourceId;

    /** The file merging type. */
    private final FileMergingType fileMergingType;

    /** Max size for a file. */
    private long maxFileSize = 32 * 1024 * 1024;

    /** Type of physical file pool. */
    private PhysicalFilePool.Type filePoolType = PhysicalFilePool.Type.NON_BLOCKING;

    /** The max space amplification that the manager should control. */
    private float maxSpaceAmplification = Float.MAX_VALUE;

    @Nullable private Executor ioExecutor = null;

    @Nullable private TaskManagerJobMetricGroup metricGroup;

    /**
     * Initialize the builder.
     *
     * @param id the id of the manager.
     */
    public FileMergingSnapshotManagerBuilder(
            JobID jobId, ResourceID tmResourceId, FileMergingType type) {
        this.jobId = jobId;
        this.tmResourceId = tmResourceId;
        this.fileMergingType = type;
    }

    /** Set the max file size. */
    public FileMergingSnapshotManagerBuilder setMaxFileSize(long maxFileSize) {
        Preconditions.checkArgument(maxFileSize > 0);
        this.maxFileSize = maxFileSize;
        return this;
    }

    /** Set the type of physical file pool. */
    public FileMergingSnapshotManagerBuilder setFilePoolType(PhysicalFilePool.Type filePoolType) {
        this.filePoolType = filePoolType;
        return this;
    }

    public FileMergingSnapshotManagerBuilder setMaxSpaceAmplification(float amplification) {
        if (amplification < 1) {
            // only valid number counts. If not valid, disable space control by setting this to
            // Float.MAX_VALUE.
            this.maxSpaceAmplification = Float.MAX_VALUE;
        } else {
            this.maxSpaceAmplification = amplification;
        }
        return this;
    }

    /**
     * Set the executor for io operation in manager. If null(default), all io operation will be
     * executed synchronously.
     */
    public FileMergingSnapshotManagerBuilder setIOExecutor(@Nullable Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
        return this;
    }

    public FileMergingSnapshotManagerBuilder setMetricGroup(TaskManagerJobMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    /**
     * Create file-merging snapshot manager based on configuration.
     *
     * @return the created manager.
     */
    public FileMergingSnapshotManager build() {
        switch (fileMergingType) {
            case MERGE_WITHIN_CHECKPOINT:
                return new WithinCheckpointFileMergingSnapshotManager(
                        String.format(ID_FORMAT, jobId, tmResourceId),
                        maxFileSize,
                        filePoolType,
                        maxSpaceAmplification,
                        ioExecutor == null ? Runnable::run : ioExecutor,
                        metricGroup == null
                                ? new UnregisteredMetricGroups
                                        .UnregisteredTaskManagerJobMetricGroup()
                                : metricGroup);
            case MERGE_ACROSS_CHECKPOINT:
                return new AcrossCheckpointFileMergingSnapshotManager(
                        String.format(ID_FORMAT, jobId, tmResourceId),
                        maxFileSize,
                        filePoolType,
                        maxSpaceAmplification,
                        ioExecutor == null ? Runnable::run : ioExecutor,
                        metricGroup == null
                                ? new UnregisteredMetricGroups
                                        .UnregisteredTaskManagerJobMetricGroup()
                                : metricGroup);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported type %s when creating file merging manager",
                                fileMergingType));
        }
    }
}
