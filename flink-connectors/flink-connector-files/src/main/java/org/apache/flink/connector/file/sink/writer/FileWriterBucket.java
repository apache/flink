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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bucket is the directory organization of the output of the {@link FileSink}.
 *
 * <p>For each incoming element in the {@code FileSink}, the user-specified {@link BucketAssigner}
 * is queried to see in which bucket this element should be written to.
 *
 * <p>This writer is responsible for writing the input data and managing the staging area used by a
 * bucket to temporarily store in-progress, uncommitted data.
 *
 * @param <IN> The type of input elements.
 */
@Internal
class FileWriterBucket<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(FileWriterBucket.class);

    private final String bucketId;

    private final Path bucketPath;

    private final BucketWriter<IN, String> bucketWriter;

    private final RollingPolicy<IN, String> rollingPolicy;

    private final OutputFileConfig outputFileConfig;

    private final String uniqueId;

    private final List<InProgressFileWriter.PendingFileRecoverable> pendingFiles =
            new ArrayList<>();

    private long partCounter;

    @Nullable private InProgressFileRecoverable inProgressFileToCleanup;

    @Nullable private InProgressFileWriter<IN, String> inProgressPart;

    /** Constructor to create a new empty bucket. */
    private FileWriterBucket(
            String bucketId,
            Path bucketPath,
            BucketWriter<IN, String> bucketWriter,
            RollingPolicy<IN, String> rollingPolicy,
            OutputFileConfig outputFileConfig) {
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.uniqueId = UUID.randomUUID().toString();
        this.partCounter = 0;
    }

    /** Constructor to restore a bucket from checkpointed state. */
    private FileWriterBucket(
            BucketWriter<IN, String> partFileFactory,
            RollingPolicy<IN, String> rollingPolicy,
            FileWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException {

        this(
                bucketState.getBucketId(),
                bucketState.getBucketPath(),
                partFileFactory,
                rollingPolicy,
                outputFileConfig);

        restoreInProgressFile(bucketState);

        // Restore pending files, this only make difference if we are
        // migrating from {@code StreamingFileSink}.
        cacheRecoveredPendingFiles(bucketState);
    }

    private void restoreInProgressFile(FileWriterBucketState state) throws IOException {
        if (!state.hasInProgressFileRecoverable()) {
            return;
        }

        // we try to resume the previous in-progress file
        InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable =
                state.getInProgressFileRecoverable();

        if (bucketWriter.getProperties().supportsResume()) {
            inProgressPart =
                    bucketWriter.resumeInProgressFileFrom(
                            bucketId,
                            inProgressFileRecoverable,
                            state.getInProgressFileCreationTime());
        } else {
            pendingFiles.add(inProgressFileRecoverable);
        }
    }

    private void cacheRecoveredPendingFiles(FileWriterBucketState state) {
        // Cache the previous pending files and send to committer on the first prepareCommit
        // operation.
        for (List<InProgressFileWriter.PendingFileRecoverable> restoredPendingRecoverables :
                state.getPendingFileRecoverablesPerCheckpoint().values()) {
            pendingFiles.addAll(restoredPendingRecoverables);
        }
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getPartCounter() {
        return partCounter;
    }

    public boolean isActive() {
        return inProgressPart != null || inProgressFileToCleanup != null || pendingFiles.size() > 0;
    }

    void merge(final FileWriterBucket<IN> bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        bucket.closePartFile();
        pendingFiles.addAll(bucket.pendingFiles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging buckets for bucket id={}", bucketId);
        }
    }

    void write(IN element, long currentTime) throws IOException {
        if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Opening new part file for bucket id={} due to element {}.",
                        bucketId,
                        element);
            }
            inProgressPart = rollPartFile(currentTime);
        }

        inProgressPart.write(element, currentTime);
    }

    List<FileSinkCommittable> prepareCommit(boolean flush) throws IOException {
        if (inProgressPart != null
                && (rollingPolicy.shouldRollOnCheckpoint(inProgressPart) || flush)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Closing in-progress part file for bucket id={} on checkpoint.", bucketId);
            }
            closePartFile();
        }

        List<FileSinkCommittable> committables = new ArrayList<>();
        pendingFiles.forEach(pendingFile -> committables.add(new FileSinkCommittable(pendingFile)));
        pendingFiles.clear();

        if (inProgressFileToCleanup != null) {
            committables.add(new FileSinkCommittable(inProgressFileToCleanup));
            inProgressFileToCleanup = null;
        }

        return committables;
    }

    FileWriterBucketState snapshotState() throws IOException {
        InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
        long inProgressFileCreationTime = Long.MAX_VALUE;

        if (inProgressPart != null) {
            inProgressFileRecoverable = inProgressPart.persist();
            inProgressFileToCleanup = inProgressFileRecoverable;
            inProgressFileCreationTime = inProgressPart.getCreationTime();
        }

        return new FileWriterBucketState(
                bucketId, bucketPath, inProgressFileCreationTime, inProgressFileRecoverable);
    }

    void onProcessingTime(long timestamp) throws IOException {
        if (inProgressPart != null
                && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Bucket {} closing in-progress part file for part file id={} due to processing time rolling policy "
                                + "(in-progress file created @ {}, last updated @ {} and current time is {}).",
                        bucketId,
                        uniqueId,
                        inProgressPart.getCreationTime(),
                        inProgressPart.getLastUpdateTime(),
                        timestamp);
            }

            closePartFile();
        }
    }

    private InProgressFileWriter<IN, String> rollPartFile(long currentTime) throws IOException {
        closePartFile();

        final Path partFilePath = assembleNewPartPath();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Opening new part file \"{}\" for bucket id={}.",
                    partFilePath.getName(),
                    bucketId);
        }

        return bucketWriter.openNewInProgressFile(bucketId, partFilePath, currentTime);
    }

    /** Constructor a new PartPath and increment the partCounter. */
    private Path assembleNewPartPath() {
        long currentPartCounter = partCounter++;
        return new Path(
                bucketPath,
                outputFileConfig.getPartPrefix()
                        + '-'
                        + uniqueId
                        + '-'
                        + currentPartCounter
                        + outputFileConfig.getPartSuffix());
    }

    private void closePartFile() throws IOException {
        if (inProgressPart != null) {
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable =
                    inProgressPart.closeForCommit();
            pendingFiles.add(pendingFileRecoverable);
            inProgressPart = null;
        }
    }

    void disposePartFile() {
        if (inProgressPart != null) {
            inProgressPart.dispose();
        }
    }

    // --------------------------- Testing Methods -----------------------------

    @VisibleForTesting
    public String getUniqueId() {
        return uniqueId;
    }

    @Nullable
    @VisibleForTesting
    InProgressFileWriter<IN, String> getInProgressPart() {
        return inProgressPart;
    }

    @VisibleForTesting
    public List<InProgressFileWriter.PendingFileRecoverable> getPendingFiles() {
        return pendingFiles;
    }

    // --------------------------- Static Factory Methods -----------------------------

    /**
     * Creates a new empty {@code Bucket}.
     *
     * @param bucketId the identifier of the bucket, as returned by the {@link BucketAssigner}.
     * @param bucketPath the path to where the part files for the bucket will be written to.
     * @param bucketWriter the {@link BucketWriter} used to write part files in the bucket.
     * @param <IN> the type of input elements to the sink.
     * @param outputFileConfig the part file configuration.
     * @return The new Bucket.
     */
    static <IN> FileWriterBucket<IN> getNew(
            final String bucketId,
            final Path bucketPath,
            final BucketWriter<IN, String> bucketWriter,
            final RollingPolicy<IN, String> rollingPolicy,
            final OutputFileConfig outputFileConfig) {
        return new FileWriterBucket<>(
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
    }

    /**
     * Restores a {@code Bucket} from the state included in the provided {@link
     * FileWriterBucketState}.
     *
     * @param bucketWriter the {@link BucketWriter} used to write part files in the bucket.
     * @param bucketState the initial state of the restored bucket.
     * @param <IN> the type of input elements to the sink.
     * @param outputFileConfig the part file configuration.
     * @return The restored Bucket.
     */
    static <IN> FileWriterBucket<IN> restore(
            final BucketWriter<IN, String> bucketWriter,
            final RollingPolicy<IN, String> rollingPolicy,
            final FileWriterBucketState bucketState,
            final OutputFileConfig outputFileConfig)
            throws IOException {
        return new FileWriterBucket<>(bucketWriter, rollingPolicy, bucketState, outputFileConfig);
    }
}
