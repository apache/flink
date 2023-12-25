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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.NotDuplicatingCheckpointStateToolset;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of durable checkpoint storage to file systems. */
public class FsCheckpointStorageAccess extends AbstractFsCheckpointStorageAccess {

    private static final String CHECKPOINT_WRITE_FILE_RATE_METRIC =
            "checkpointWriteFileBytesPerSecond";

    private static final String CHECKPOINT_WRITE_FILE_LATENCY_METRIC = "checkpointWriteFileLatency";

    private static final String CHECKPOINT_CLOSE_FILE_LATENCY_METRIC = "checkpointCloseFileLatency";

    protected final FileSystem fileSystem;

    protected final Path checkpointsDirectory;

    protected final Path sharedStateDirectory;

    protected final Path taskOwnedStateDirectory;

    private final int fileSizeThreshold;

    private final int writeBufferSize;

    private boolean baseLocationsInitialized = false;

    private final CheckpointFileAccessStatistic checkpointFileAccessStatistic;

    public FsCheckpointStorageAccess(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        this(
                checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
    }

    public FsCheckpointStorageAccess(
            FileSystem fs,
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        super(jobId, defaultSavepointDirectory);

        checkArgument(fileSizeThreshold >= 0);
        checkArgument(writeBufferSize >= 0);

        this.fileSystem = checkNotNull(fs);
        this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId);
        this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
        this.taskOwnedStateDirectory =
                new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
        this.fileSizeThreshold = fileSizeThreshold;
        this.writeBufferSize = writeBufferSize;
        this.checkpointFileAccessStatistic = new CheckpointFileAccessStatistic();
    }

    public void registerMetrics(MetricGroup metricGroup) {
        metricGroup.meter(
                CHECKPOINT_WRITE_FILE_RATE_METRIC,
                new CheckpointWriteFileRate(checkpointFileAccessStatistic));
        metricGroup.gauge(
                CHECKPOINT_WRITE_FILE_LATENCY_METRIC,
                new CheckpointWriteFileLatency(checkpointFileAccessStatistic));
        metricGroup.gauge(
                CHECKPOINT_CLOSE_FILE_LATENCY_METRIC,
                new CheckpointCloseFileLatency(checkpointFileAccessStatistic));
    }

    /** Metric for interaction performance with the external file system. */
    public static class CheckpointFileAccessStatistic {
        private final Object lock = new Object();

        private long writeBytes = 0;
        private long writeLatency = 0;
        private long writeCount = 0;
        private long closeLatency = 0;
        private long closeCount = 0;

        public void updateWriteFileStatistics(long writeBytes, long writeLatency, long writeCount) {
            synchronized (lock) {
                this.writeBytes += writeBytes;
                this.writeLatency += writeLatency;
                this.writeCount += writeCount;
            }
        }

        public void updateCloseFileStatistics(long closeLatency) {
            synchronized (lock) {
                this.closeLatency += closeLatency;
                this.closeCount++;
            }
        }

        public long getAndClearWriteBytes() {
            synchronized (lock) {
                long recentWriteBytes = this.writeBytes;
                this.writeBytes = 0;
                return recentWriteBytes;
            }
        }

        public double getAndClearAvgWriteLatency() {
            synchronized (lock) {
                long recentWriteLatency = this.writeLatency;
                long recentWriteCount = this.writeCount;
                this.writeLatency = 0;
                this.writeCount = 0;
                return recentWriteCount > 0
                        ? ((double) recentWriteLatency / recentWriteCount)
                        : 0.0;
            }
        }

        public double getAndClearAvgCloseLatency() {
            synchronized (lock) {
                long recentCloseLatency = this.closeLatency;
                long recentCloseCount = this.closeCount;
                this.closeLatency = 0;
                this.closeCount = 0;
                return recentCloseCount > 0
                        ? ((double) recentCloseLatency / recentCloseCount)
                        : 0.0;
            }
        }

        @Override
        public String toString() {
            return "CheckpointFileAccessStatistic{"
                    + ", writeBytes="
                    + writeBytes
                    + ", writeLatency="
                    + writeLatency
                    + ", writeCount="
                    + writeCount
                    + ", closeLatency="
                    + closeLatency
                    + ", closeCount="
                    + closeCount
                    + '}';
        }
    }

    private static class CheckpointWriteFileRate extends MeterView {

        private final CheckpointFileAccessStatistic checkpointFileAccessStatistic;

        public CheckpointWriteFileRate(
                CheckpointFileAccessStatistic checkpointFileAccessStatistic) {
            super(new SimpleCounter());
            this.checkpointFileAccessStatistic = checkpointFileAccessStatistic;
        }

        @Override
        public void update() {
            markEvent(checkpointFileAccessStatistic.getAndClearWriteBytes());
            super.update();
        }
    }

    private static class CheckpointWriteFileLatency implements Gauge<Double> {

        private final CheckpointFileAccessStatistic checkpointFileAccessStatistic;

        public CheckpointWriteFileLatency(
                CheckpointFileAccessStatistic checkpointFileAccessStatistic) {
            this.checkpointFileAccessStatistic = checkpointFileAccessStatistic;
        }

        @Override
        public Double getValue() {
            return checkpointFileAccessStatistic.getAndClearAvgWriteLatency();
        }
    }

    private static class CheckpointCloseFileLatency implements Gauge<Double> {

        private final CheckpointFileAccessStatistic checkpointFileAccessStatistic;

        public CheckpointCloseFileLatency(
                CheckpointFileAccessStatistic checkpointFileAccessStatistic) {
            this.checkpointFileAccessStatistic = checkpointFileAccessStatistic;
        }

        @Override
        public Double getValue() {
            return checkpointFileAccessStatistic.getAndClearAvgCloseLatency();
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    Path getCheckpointsDirectory() {
        return checkpointsDirectory;
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorage implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsHighlyAvailableStorage() {
        return true;
    }

    @Override
    public void initializeBaseLocationsForCheckpoint() throws IOException {
        if (!fileSystem.mkdirs(sharedStateDirectory)) {
            throw new IOException(
                    "Failed to create directory for shared state: " + sharedStateDirectory);
        }
        if (!fileSystem.mkdirs(taskOwnedStateDirectory)) {
            throw new IOException(
                    "Failed to create directory for task owned state: " + taskOwnedStateDirectory);
        }
    }

    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
            throws IOException {
        checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %s.", checkpointId);

        // prepare all the paths needed for the checkpoints
        final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

        // create the checkpoint exclusive directory
        fileSystem.mkdirs(checkpointDir);

        return new FsCheckpointStorageLocation(
                fileSystem,
                checkpointDir,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                CheckpointStorageLocationReference.getDefault(),
                fileSizeThreshold,
                writeBufferSize,
                checkpointFileAccessStatistic);
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException {

        if (reference.isDefaultReference()) {
            // default reference, construct the default location for that particular checkpoint
            final Path checkpointDir =
                    createCheckpointDirectory(checkpointsDirectory, checkpointId);

            return new FsCheckpointStorageLocation(
                    fileSystem,
                    checkpointDir,
                    sharedStateDirectory,
                    taskOwnedStateDirectory,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize,
                    checkpointFileAccessStatistic);
        } else {
            // location encoded in the reference
            final Path path = decodePathFromReference(reference);

            return new FsCheckpointStorageLocation(
                    path.getFileSystem(),
                    path,
                    path,
                    path,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize,
                    checkpointFileAccessStatistic);
        }
    }

    @Override
    public CheckpointStateOutputStream createTaskOwnedStateStream() {
        // as the comment of CheckpointStorageWorkerView#createTaskOwnedStateStream said we may
        // change into shared state,
        // so we use CheckpointedStateScope.SHARED here.
        return new FsCheckpointStateOutputStream(
                taskOwnedStateDirectory, fileSystem, writeBufferSize, fileSizeThreshold);
    }

    @Override
    public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
        if (fileSystem instanceof DuplicatingFileSystem) {
            return new FsCheckpointStateToolset(
                    taskOwnedStateDirectory, (DuplicatingFileSystem) fileSystem);
        } else {
            return new NotDuplicatingCheckpointStateToolset();
        }
    }

    @Override
    protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
        final CheckpointStorageLocationReference reference = encodePathAsReference(location);
        return new FsCheckpointStorageLocation(
                fs,
                location,
                location,
                location,
                reference,
                fileSizeThreshold,
                writeBufferSize,
                checkpointFileAccessStatistic);
    }

    public FsMergingCheckpointStorageAccess toFileMergingStorage(
            FileMergingSnapshotManager mergingSnapshotManager, Environment environment)
            throws IOException {
        return new FsMergingCheckpointStorageAccess(
                fileSystem,
                checkpointsDirectory.getParent(),
                getDefaultSavepointDirectory(),
                environment.getJobID(),
                fileSizeThreshold,
                writeBufferSize,
                mergingSnapshotManager,
                environment);
    }
}
