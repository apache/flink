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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The manager of the different active buckets in the {@link StreamingFileSink}.
 *
 * <p>This class is responsible for all bucket-related operations and the actual {@link
 * StreamingFileSink} is just plugging in the functionality offered by this class to the lifecycle
 * of the operator.
 *
 * @param <IN> The type of input elements.
 * @param <BucketID> The type of ids for the buckets, as returned by the {@link BucketAssigner}.
 */
@Internal
public class Buckets<IN, BucketID> {

    private static final Logger LOG = LoggerFactory.getLogger(Buckets.class);

    // ------------------------ configuration fields --------------------------

    private final Path basePath;

    private final BucketFactory<IN, BucketID> bucketFactory;

    private final BucketAssigner<IN, BucketID> bucketAssigner;

    private final BucketWriter<IN, BucketID> bucketWriter;

    private final RollingPolicy<IN, BucketID> rollingPolicy;

    // --------------------------- runtime fields -----------------------------

    private final int subtaskIndex;

    private final Buckets.BucketerContext bucketerContext;

    private final Map<BucketID, Bucket<IN, BucketID>> activeBuckets;

    private long maxPartCounter;

    private final OutputFileConfig outputFileConfig;

    @Nullable private BucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener;

    @Nullable private FileLifeCycleListener<BucketID> fileLifeCycleListener;

    // --------------------------- State Related Fields -----------------------------

    private final BucketStateSerializer<BucketID> bucketStateSerializer;

    /**
     * A constructor creating a new empty bucket manager.
     *
     * @param basePath The base path for our buckets.
     * @param bucketAssigner The {@link BucketAssigner} provided by the user.
     * @param bucketFactory The {@link BucketFactory} to be used to create buckets.
     * @param bucketWriter The {@link BucketWriter} to be used when writing data.
     * @param rollingPolicy The {@link RollingPolicy} as specified by the user.
     */
    public Buckets(
            final Path basePath,
            final BucketAssigner<IN, BucketID> bucketAssigner,
            final BucketFactory<IN, BucketID> bucketFactory,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            final int subtaskIndex,
            final OutputFileConfig outputFileConfig) {

        this.basePath = Preconditions.checkNotNull(basePath);
        this.bucketAssigner = Preconditions.checkNotNull(bucketAssigner);
        this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
        this.bucketWriter = Preconditions.checkNotNull(bucketWriter);
        this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
        this.subtaskIndex = subtaskIndex;

        this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);

        this.activeBuckets = new HashMap<>();
        this.bucketerContext = new Buckets.BucketerContext();

        this.bucketStateSerializer =
                new BucketStateSerializer<>(
                        bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                        bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                        bucketAssigner.getSerializer());
        this.maxPartCounter = 0L;
    }

    public void setBucketLifeCycleListener(
            BucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener) {
        this.bucketLifeCycleListener = Preconditions.checkNotNull(bucketLifeCycleListener);
    }

    public void setFileLifeCycleListener(FileLifeCycleListener<BucketID> fileLifeCycleListener) {
        this.fileLifeCycleListener = Preconditions.checkNotNull(fileLifeCycleListener);
    }

    /**
     * Initializes the state after recovery from a failure.
     *
     * <p>During this process:
     *
     * <ol>
     *   <li>we set the initial value for part counter to the maximum value used before across all
     *       tasks and buckets. This guarantees that we do not overwrite valid data,
     *   <li>we commit any pending files for previous checkpoints (previous to the last successful
     *       one from which we restore),
     *   <li>we resume writing to the previous in-progress file of each bucket, and
     *   <li>if we receive multiple states for the same bucket, we merge them.
     * </ol>
     *
     * @param bucketStates the state holding recovered state about active buckets.
     * @param partCounterState the state holding the max previously used part counters.
     * @throws Exception if anything goes wrong during retrieving the state or restoring/committing
     *     of any in-progress/pending part files
     */
    public void initializeState(
            final ListState<byte[]> bucketStates, final ListState<Long> partCounterState)
            throws Exception {

        initializePartCounter(partCounterState);

        LOG.info(
                "Subtask {} initializing its state (max part counter={}).",
                subtaskIndex,
                maxPartCounter);

        initializeActiveBuckets(bucketStates);
    }

    private void initializePartCounter(final ListState<Long> partCounterState) throws Exception {
        long maxCounter = 0L;
        for (long partCounter : partCounterState.get()) {
            maxCounter = Math.max(partCounter, maxCounter);
        }
        maxPartCounter = maxCounter;
    }

    private void initializeActiveBuckets(final ListState<byte[]> bucketStates) throws Exception {
        for (byte[] serializedRecoveredState : bucketStates.get()) {
            final BucketState<BucketID> recoveredState =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            bucketStateSerializer, serializedRecoveredState);
            handleRestoredBucketState(recoveredState);
        }
    }

    private void handleRestoredBucketState(final BucketState<BucketID> recoveredState)
            throws Exception {
        final BucketID bucketId = recoveredState.getBucketId();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Subtask {} restoring: {}", subtaskIndex, recoveredState);
        }

        final Bucket<IN, BucketID> restoredBucket =
                bucketFactory.restoreBucket(
                        subtaskIndex,
                        maxPartCounter,
                        bucketWriter,
                        rollingPolicy,
                        recoveredState,
                        fileLifeCycleListener,
                        outputFileConfig);

        updateActiveBucketId(bucketId, restoredBucket);
    }

    private void updateActiveBucketId(
            final BucketID bucketId, final Bucket<IN, BucketID> restoredBucket) throws IOException {
        if (!restoredBucket.isActive()) {
            notifyBucketInactive(restoredBucket);
            return;
        }

        final Bucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
        if (bucket != null) {
            bucket.merge(restoredBucket);
        } else {
            activeBuckets.put(bucketId, restoredBucket);
        }
    }

    public void commitUpToCheckpoint(final long checkpointId) throws IOException {
        final Iterator<Map.Entry<BucketID, Bucket<IN, BucketID>>> activeBucketIt =
                activeBuckets.entrySet().iterator();

        LOG.info(
                "Subtask {} received completion notification for checkpoint with id={}.",
                subtaskIndex,
                checkpointId);

        while (activeBucketIt.hasNext()) {
            final Bucket<IN, BucketID> bucket = activeBucketIt.next().getValue();
            bucket.onSuccessfulCompletionOfCheckpoint(checkpointId);

            if (!bucket.isActive()) {
                // We've dealt with all the pending files and the writer for this bucket is not
                // currently open.
                // Therefore this bucket is currently inactive and we can remove it from our state.
                activeBucketIt.remove();
                notifyBucketInactive(bucket);
            }
        }
    }

    public void snapshotState(
            final long checkpointId,
            final ListState<byte[]> bucketStatesContainer,
            final ListState<Long> partCounterStateContainer)
            throws Exception {

        Preconditions.checkState(
                bucketWriter != null && bucketStateSerializer != null,
                "sink has not been initialized");

        LOG.info(
                "Subtask {} checkpointing for checkpoint with id={} (max part counter={}).",
                subtaskIndex,
                checkpointId,
                maxPartCounter);

        bucketStatesContainer.clear();
        partCounterStateContainer.clear();

        snapshotActiveBuckets(checkpointId, bucketStatesContainer);
        partCounterStateContainer.add(maxPartCounter);
    }

    private void snapshotActiveBuckets(
            final long checkpointId, final ListState<byte[]> bucketStatesContainer)
            throws Exception {

        for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
            final BucketState<BucketID> bucketState = bucket.onReceptionOfCheckpoint(checkpointId);

            final byte[] serializedBucketState =
                    SimpleVersionedSerialization.writeVersionAndSerialize(
                            bucketStateSerializer, bucketState);

            bucketStatesContainer.add(serializedBucketState);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Subtask {} checkpointing: {}", subtaskIndex, bucketState);
            }
        }
    }

    @VisibleForTesting
    public Bucket<IN, BucketID> onElement(final IN value, final SinkFunction.Context context)
            throws Exception {
        return onElement(
                value,
                context.currentProcessingTime(),
                context.timestamp(),
                context.currentWatermark());
    }

    public Bucket<IN, BucketID> onElement(
            final IN value,
            final long currentProcessingTime,
            @Nullable final Long elementTimestamp,
            final long currentWatermark)
            throws Exception {
        // setting the values in the bucketer context
        bucketerContext.update(elementTimestamp, currentWatermark, currentProcessingTime);

        final BucketID bucketId = bucketAssigner.getBucketId(value, bucketerContext);
        final Bucket<IN, BucketID> bucket = getOrCreateBucketForBucketId(bucketId);
        bucket.write(value, currentProcessingTime);

        // we update the global max counter here because as buckets become inactive and
        // get removed from the list of active buckets, at the time when we want to create
        // another part file for the bucket, if we start from 0 we may overwrite previous parts.

        this.maxPartCounter = Math.max(maxPartCounter, bucket.getPartCounter());
        return bucket;
    }

    private Bucket<IN, BucketID> getOrCreateBucketForBucketId(final BucketID bucketId)
            throws IOException {
        Bucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
        if (bucket == null) {
            final Path bucketPath = assembleBucketPath(bucketId);
            bucket =
                    bucketFactory.getNewBucket(
                            subtaskIndex,
                            bucketId,
                            bucketPath,
                            maxPartCounter,
                            bucketWriter,
                            rollingPolicy,
                            fileLifeCycleListener,
                            outputFileConfig);
            activeBuckets.put(bucketId, bucket);
            notifyBucketCreate(bucket);
        }
        return bucket;
    }

    public void onProcessingTime(long timestamp) throws Exception {
        for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
            bucket.onProcessingTime(timestamp);
        }
    }

    public void close() {
        if (activeBuckets != null) {
            activeBuckets.values().forEach(Bucket::disposePartFile);
        }
    }

    private Path assembleBucketPath(BucketID bucketId) {
        final String child = bucketId.toString();
        if ("".equals(child)) {
            return basePath;
        }
        return new Path(basePath, child);
    }

    private void notifyBucketCreate(Bucket<IN, BucketID> bucket) {
        if (bucketLifeCycleListener != null) {
            bucketLifeCycleListener.bucketCreated(bucket);
        }
    }

    private void notifyBucketInactive(Bucket<IN, BucketID> bucket) {
        if (bucketLifeCycleListener != null) {
            bucketLifeCycleListener.bucketInactive(bucket);
        }
    }

    /**
     * The {@link BucketAssigner.Context} exposed to the {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} whenever a new incoming element arrives.
     */
    private static final class BucketerContext implements BucketAssigner.Context {

        @Nullable private Long elementTimestamp;

        private long currentWatermark;

        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = Long.MIN_VALUE;
            this.currentProcessingTime = Long.MIN_VALUE;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = processingTime;
        }

        @Override
        public long currentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return elementTimestamp;
        }
    }

    // --------------------------- Testing Methods -----------------------------

    @VisibleForTesting
    public long getMaxPartCounter() {
        return maxPartCounter;
    }

    @VisibleForTesting
    Map<BucketID, Bucket<IN, BucketID>> getActiveBuckets() {
        return activeBuckets;
    }
}
