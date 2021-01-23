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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bucket is the directory organization of the output of the {@link StreamingFileSink}.
 *
 * <p>For each incoming element in the {@code StreamingFileSink}, the user-specified {@link
 * BucketAssigner} is queried to see in which bucket this element should be written to.
 */
@Internal
public class Bucket<IN, BucketID> {

    private static final Logger LOG = LoggerFactory.getLogger(Bucket.class);

    private final BucketID bucketId;

    private final Path bucketPath;

    private final int subtaskIndex;

    private final BucketWriter<IN, BucketID> bucketWriter;

    private final RollingPolicy<IN, BucketID> rollingPolicy;

    private final NavigableMap<Long, InProgressFileWriter.InProgressFileRecoverable>
            inProgressFileRecoverablesPerCheckpoint;

    private final NavigableMap<Long, List<InProgressFileWriter.PendingFileRecoverable>>
            pendingFileRecoverablesPerCheckpoint;

    private final OutputFileConfig outputFileConfig;

    @Nullable private final FileLifeCycleListener<BucketID> fileListener;

    private long partCounter;

    @Nullable private InProgressFileWriter<IN, BucketID> inProgressPart;

    private List<InProgressFileWriter.PendingFileRecoverable>
            pendingFileRecoverablesForCurrentCheckpoint;

    /** Constructor to create a new empty bucket. */
    private Bucket(
            final int subtaskIndex,
            final BucketID bucketId,
            final Path bucketPath,
            final long initialPartCounter,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener,
            final OutputFileConfig outputFileConfig) {
        this.subtaskIndex = subtaskIndex;
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
        this.partCounter = initialPartCounter;
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.fileListener = fileListener;

        this.pendingFileRecoverablesForCurrentCheckpoint = new ArrayList<>();
        this.pendingFileRecoverablesPerCheckpoint = new TreeMap<>();
        this.inProgressFileRecoverablesPerCheckpoint = new TreeMap<>();

        this.outputFileConfig = checkNotNull(outputFileConfig);
    }

    /** Constructor to restore a bucket from checkpointed state. */
    private Bucket(
            final int subtaskIndex,
            final long initialPartCounter,
            final BucketWriter<IN, BucketID> partFileFactory,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            final BucketState<BucketID> bucketState,
            @Nullable final FileLifeCycleListener<BucketID> fileListener,
            final OutputFileConfig outputFileConfig)
            throws IOException {

        this(
                subtaskIndex,
                bucketState.getBucketId(),
                bucketState.getBucketPath(),
                initialPartCounter,
                partFileFactory,
                rollingPolicy,
                fileListener,
                outputFileConfig);

        restoreInProgressFile(bucketState);
        commitRecoveredPendingFiles(bucketState);
    }

    private void restoreInProgressFile(final BucketState<BucketID> state) throws IOException {
        if (!state.hasInProgressFileRecoverable()) {
            return;
        }

        // we try to resume the previous in-progress file
        final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable =
                state.getInProgressFileRecoverable();

        if (bucketWriter.getProperties().supportsResume()) {
            inProgressPart =
                    bucketWriter.resumeInProgressFileFrom(
                            bucketId,
                            inProgressFileRecoverable,
                            state.getInProgressFileCreationTime());
        } else {
            // if the writer does not support resume, then we close the
            // in-progress part and commit it, as done in the case of pending files.
            bucketWriter.recoverPendingFile(inProgressFileRecoverable).commitAfterRecovery();
        }
    }

    private void commitRecoveredPendingFiles(final BucketState<BucketID> state) throws IOException {

        // we commit pending files for checkpoints that precess the last successful one, from which
        // we are recovering
        for (List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables :
                state.getPendingFileRecoverablesPerCheckpoint().values()) {
            for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable :
                    pendingFileRecoverables) {
                bucketWriter.recoverPendingFile(pendingFileRecoverable).commitAfterRecovery();
            }
        }
    }

    public BucketID getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getPartCounter() {
        return partCounter;
    }

    boolean isActive() {
        return inProgressPart != null
                || !pendingFileRecoverablesForCurrentCheckpoint.isEmpty()
                || !pendingFileRecoverablesPerCheckpoint.isEmpty();
    }

    void merge(final Bucket<IN, BucketID> bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        // There should be no pending files in the "to-merge" states.
        // The reason is that:
        // 1) the pendingFileRecoverablesForCurrentCheckpoint is emptied whenever we take a
        // Recoverable (see prepareBucketForCheckpointing()).
        //    So a Recoverable, including the one we are recovering from, will never contain such
        // files.
        // 2) the files in pendingFileRecoverablesPerCheckpoint are committed upon recovery (see
        // commitRecoveredPendingFiles()).

        checkState(bucket.pendingFileRecoverablesForCurrentCheckpoint.isEmpty());
        checkState(bucket.pendingFileRecoverablesPerCheckpoint.isEmpty());

        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = bucket.closePartFile();
        if (pendingFileRecoverable != null) {
            pendingFileRecoverablesForCurrentCheckpoint.add(pendingFileRecoverable);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Subtask {} merging buckets for bucket id={}", subtaskIndex, bucketId);
        }
    }

    void write(IN element, long currentTime) throws IOException {
        if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Subtask {} closing in-progress part file for bucket id={} due to element {}.",
                        subtaskIndex,
                        bucketId,
                        element);
            }

            inProgressPart = rollPartFile(currentTime);
        }
        inProgressPart.write(element, currentTime);
    }

    private InProgressFileWriter<IN, BucketID> rollPartFile(final long currentTime)
            throws IOException {
        closePartFile();

        final Path partFilePath = assembleNewPartPath();

        if (fileListener != null) {
            fileListener.onPartFileOpened(bucketId, partFilePath);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Subtask {} opening new part file \"{}\" for bucket id={}.",
                    subtaskIndex,
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
                        + subtaskIndex
                        + '-'
                        + currentPartCounter
                        + outputFileConfig.getPartSuffix());
    }

    private InProgressFileWriter.PendingFileRecoverable closePartFile() throws IOException {
        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = null;
        if (inProgressPart != null) {
            pendingFileRecoverable = inProgressPart.closeForCommit();
            pendingFileRecoverablesForCurrentCheckpoint.add(pendingFileRecoverable);
            inProgressPart = null;
        }
        return pendingFileRecoverable;
    }

    void disposePartFile() {
        if (inProgressPart != null) {
            inProgressPart.dispose();
        }
    }

    BucketState<BucketID> onReceptionOfCheckpoint(long checkpointId) throws IOException {
        prepareBucketForCheckpointing(checkpointId);

        InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
        long inProgressFileCreationTime = Long.MAX_VALUE;

        if (inProgressPart != null) {
            inProgressFileRecoverable = inProgressPart.persist();
            inProgressFileCreationTime = inProgressPart.getCreationTime();
            this.inProgressFileRecoverablesPerCheckpoint.put(
                    checkpointId, inProgressFileRecoverable);
        }

        return new BucketState<>(
                bucketId,
                bucketPath,
                inProgressFileCreationTime,
                inProgressFileRecoverable,
                pendingFileRecoverablesPerCheckpoint);
    }

    private void prepareBucketForCheckpointing(long checkpointId) throws IOException {
        if (inProgressPart != null && rollingPolicy.shouldRollOnCheckpoint(inProgressPart)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Subtask {} closing in-progress part file for bucket id={} on checkpoint.",
                        subtaskIndex,
                        bucketId);
            }
            closePartFile();
        }

        if (!pendingFileRecoverablesForCurrentCheckpoint.isEmpty()) {
            pendingFileRecoverablesPerCheckpoint.put(
                    checkpointId, pendingFileRecoverablesForCurrentCheckpoint);
            pendingFileRecoverablesForCurrentCheckpoint = new ArrayList<>();
        }
    }

    void onSuccessfulCompletionOfCheckpoint(long checkpointId) throws IOException {
        checkNotNull(bucketWriter);

        Iterator<Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>>> it =
                pendingFileRecoverablesPerCheckpoint
                        .headMap(checkpointId, true)
                        .entrySet()
                        .iterator();

        while (it.hasNext()) {
            Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>> entry = it.next();

            for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable :
                    entry.getValue()) {
                bucketWriter.recoverPendingFile(pendingFileRecoverable).commit();
            }
            it.remove();
        }

        cleanupInProgressFileRecoverables(checkpointId);
    }

    private void cleanupInProgressFileRecoverables(long checkpointId) throws IOException {
        Iterator<Map.Entry<Long, InProgressFileWriter.InProgressFileRecoverable>> it =
                inProgressFileRecoverablesPerCheckpoint
                        .headMap(checkpointId, false)
                        .entrySet()
                        .iterator();

        while (it.hasNext()) {
            final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable =
                    it.next().getValue();

            // this check is redundant, as we only put entries in the
            // inProgressFileRecoverablesPerCheckpoint map
            // list when the requiresCleanupOfInProgressFileRecoverableState() returns true, but
            // having it makes
            // the code more readable.

            final boolean successfullyDeleted =
                    bucketWriter.cleanupInProgressFileRecoverable(inProgressFileRecoverable);
            if (LOG.isDebugEnabled() && successfullyDeleted) {
                LOG.debug(
                        "Subtask {} successfully deleted incomplete part for bucket id={}.",
                        subtaskIndex,
                        bucketId);
            }
            it.remove();
        }
    }

    void onProcessingTime(long timestamp) throws IOException {
        if (inProgressPart != null
                && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy "
                                + "(in-progress file created @ {}, last updated @ {} and current time is {}).",
                        subtaskIndex,
                        bucketId,
                        inProgressPart.getCreationTime(),
                        inProgressPart.getLastUpdateTime(),
                        timestamp);
            }
            closePartFile();
        }
    }

    // --------------------------- Testing Methods -----------------------------

    @VisibleForTesting
    Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
            getPendingFileRecoverablesPerCheckpoint() {
        return pendingFileRecoverablesPerCheckpoint;
    }

    @Nullable
    @VisibleForTesting
    InProgressFileWriter<IN, BucketID> getInProgressPart() {
        return inProgressPart;
    }

    @VisibleForTesting
    List<InProgressFileWriter.PendingFileRecoverable>
            getPendingFileRecoverablesForCurrentCheckpoint() {
        return pendingFileRecoverablesForCurrentCheckpoint;
    }

    // --------------------------- Static Factory Methods -----------------------------

    /**
     * Creates a new empty {@code Bucket}.
     *
     * @param subtaskIndex the index of the subtask creating the bucket.
     * @param bucketId the identifier of the bucket, as returned by the {@link BucketAssigner}.
     * @param bucketPath the path to where the part files for the bucket will be written to.
     * @param initialPartCounter the initial counter for the part files of the bucket.
     * @param bucketWriter the {@link BucketWriter} used to write part files in the bucket.
     * @param rollingPolicy the policy based on which a bucket rolls its currently open part file
     *     and opens a new one.
     * @param fileListener the listener about the status of file.
     * @param <IN> the type of input elements to the sink.
     * @param <BucketID> the type of the identifier of the bucket, as returned by the {@link
     *     BucketAssigner}
     * @param outputFileConfig the part file configuration.
     * @return The new Bucket.
     */
    static <IN, BucketID> Bucket<IN, BucketID> getNew(
            final int subtaskIndex,
            final BucketID bucketId,
            final Path bucketPath,
            final long initialPartCounter,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener,
            final OutputFileConfig outputFileConfig) {
        return new Bucket<>(
                subtaskIndex,
                bucketId,
                bucketPath,
                initialPartCounter,
                bucketWriter,
                rollingPolicy,
                fileListener,
                outputFileConfig);
    }

    /**
     * Restores a {@code Bucket} from the state included in the provided {@link BucketState}.
     *
     * @param subtaskIndex the index of the subtask creating the bucket.
     * @param initialPartCounter the initial counter for the part files of the bucket.
     * @param bucketWriter the {@link BucketWriter} used to write part files in the bucket.
     * @param rollingPolicy the policy based on which a bucket rolls its currently open part file
     *     and opens a new one.
     * @param bucketState the initial state of the restored bucket.
     * @param fileListener the listener about the status of file.
     * @param <IN> the type of input elements to the sink.
     * @param <BucketID> the type of the identifier of the bucket, as returned by the {@link
     *     BucketAssigner}
     * @param outputFileConfig the part file configuration.
     * @return The restored Bucket.
     */
    static <IN, BucketID> Bucket<IN, BucketID> restore(
            final int subtaskIndex,
            final long initialPartCounter,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            final BucketState<BucketID> bucketState,
            @Nullable final FileLifeCycleListener<BucketID> fileListener,
            final OutputFileConfig outputFileConfig)
            throws IOException {
        return new Bucket<>(
                subtaskIndex,
                initialPartCounter,
                bucketWriter,
                rollingPolicy,
                bucketState,
                fileListener,
                outputFileConfig);
    }
}
