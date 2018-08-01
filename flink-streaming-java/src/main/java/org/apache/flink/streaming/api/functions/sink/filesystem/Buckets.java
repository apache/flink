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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
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
 * <p>This class is responsible for all bucket-related operations and the actual
 * {@link StreamingFileSink} is just plugging in the functionality offered by
 * this class to the lifecycle of the operator.
 *
 * @param <IN> The type of input elements.
 * @param <BucketID> The type of ids for the buckets, as returned by the {@link Bucketer}.
 */
public class Buckets<IN, BucketID> {

	private static final Logger LOG = LoggerFactory.getLogger(Buckets.class);

	// ------------------------ configuration fields --------------------------

	private final Path basePath;

	private final BucketFactory<IN, BucketID> bucketFactory;

	private final Bucketer<IN, BucketID> bucketer;

	private final PartFileWriter.PartFileFactory<IN, BucketID> partFileWriterFactory;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	// --------------------------- runtime fields -----------------------------

	private final int subtaskIndex;

	private final Buckets.BucketerContext bucketerContext;

	private final Map<BucketID, Bucket<IN, BucketID>> activeBuckets;

	private long maxPartCounterUsed;

	private final RecoverableWriter fileSystemWriter;

	// --------------------------- State Related Fields -----------------------------

	private final BucketStateSerializer<BucketID> bucketStateSerializer;

	/**
	 * A private constructor creating a new empty bucket manager.
	 *
	 * @param basePath The base path for our buckets.
	 * @param bucketer The {@link Bucketer} provided by the user.
	 * @param bucketFactory The {@link BucketFactory} to be used to create buckets.
	 * @param partFileWriterFactory The {@link PartFileWriter.PartFileFactory} to be used when writing data.
	 * @param rollingPolicy The {@link RollingPolicy} as specified by the user.
	 */
	Buckets(
			final Path basePath,
			final Bucketer<IN, BucketID> bucketer,
			final BucketFactory<IN, BucketID> bucketFactory,
			final PartFileWriter.PartFileFactory<IN, BucketID> partFileWriterFactory,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final int subtaskIndex) throws IOException {

		this.basePath = Preconditions.checkNotNull(basePath);
		this.bucketer = Preconditions.checkNotNull(bucketer);
		this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
		this.partFileWriterFactory = Preconditions.checkNotNull(partFileWriterFactory);
		this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
		this.subtaskIndex = subtaskIndex;

		this.activeBuckets = new HashMap<>();
		this.bucketerContext = new Buckets.BucketerContext();

		this.fileSystemWriter = FileSystem.get(basePath.toUri()).createRecoverableWriter();
		this.bucketStateSerializer = new BucketStateSerializer<>(
				fileSystemWriter.getResumeRecoverableSerializer(),
				fileSystemWriter.getCommitRecoverableSerializer(),
				bucketer.getSerializer()
		);

		this.maxPartCounterUsed = 0L;
	}

	/**
	 * Initializes the state after recovery from a failure.
	 * @param bucketStates the state holding recovered state about active buckets.
	 * @param partCounterState the state holding the max previously used part counters.
	 * @throws Exception
	 */
	void initializeState(final ListState<byte[]> bucketStates, final ListState<Long> partCounterState) throws Exception {

		// When resuming after a failure:
		// 1) we get the max part counter used before in order to make sure that we do not overwrite valid data
		// 2) we commit any pending files for previous checkpoints (previous to the last successful one)
		// 3) we resume writing to the previous in-progress file of each bucket, and
		// 4) if we receive multiple states for the same bucket, we merge them.

		// get the max counter
		long maxCounter = 0L;
		for (long partCounter: partCounterState.get()) {
			maxCounter = Math.max(partCounter, maxCounter);
		}
		maxPartCounterUsed = maxCounter;

		// get the restored buckets
		for (byte[] recoveredState : bucketStates.get()) {
			final BucketState<BucketID> bucketState = SimpleVersionedSerialization.readVersionAndDeSerialize(
					bucketStateSerializer, recoveredState);

			final BucketID bucketId = bucketState.getBucketId();

			LOG.info("Recovered bucket for {}", bucketId);

			final Bucket<IN, BucketID> restoredBucket = bucketFactory.restoreBucket(
					fileSystemWriter,
					subtaskIndex,
					maxPartCounterUsed,
					partFileWriterFactory,
					bucketState
			);

			final Bucket<IN, BucketID> existingBucket = activeBuckets.get(bucketId);
			if (existingBucket == null) {
				activeBuckets.put(bucketId, restoredBucket);
			} else {
				existingBucket.merge(restoredBucket);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("{} idx {} restored state for bucket {}", getClass().getSimpleName(),
						subtaskIndex, assembleBucketPath(bucketId));
			}
		}
	}

	void publishUpToCheckpoint(long checkpointId) throws IOException {
		final Iterator<Map.Entry<BucketID, Bucket<IN, BucketID>>> activeBucketIt =
				activeBuckets.entrySet().iterator();

		while (activeBucketIt.hasNext()) {
			Bucket<IN, BucketID> bucket = activeBucketIt.next().getValue();
			bucket.onCheckpointAcknowledgment(checkpointId);

			if (!bucket.isActive()) {
				// We've dealt with all the pending files and the writer for this bucket is not currently open.
				// Therefore this bucket is currently inactive and we can remove it from our state.
				activeBucketIt.remove();
			}
		}
	}

	void snapshotState(
			final long checkpointId,
			final ListState<byte[]> bucketStates,
			final ListState<Long> partCounterState) throws Exception {

		Preconditions.checkState(
				fileSystemWriter != null && bucketStateSerializer != null,
				"sink has not been initialized"
		);

		for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
			final PartFileInfo<BucketID> info = bucket.getInProgressPartInfo();

			if (info != null && rollingPolicy.shouldRollOnCheckpoint(info)) {
				bucket.closePartFile();
			}

			final BucketState<BucketID> bucketState = bucket.onCheckpoint(checkpointId);
			bucketStates.add(SimpleVersionedSerialization.writeVersionAndSerialize(bucketStateSerializer, bucketState));
		}

		partCounterState.add(maxPartCounterUsed);
	}

	/**
	 * Called on every incoming element to write it to its final location.
	 * @param value the element itself.
	 * @param context the {@link SinkFunction.Context context} available to the sink function.
	 * @throws Exception
	 */
	void onElement(IN value, SinkFunction.Context context) throws Exception {
		final long currentProcessingTime = context.currentProcessingTime();

		// setting the values in the bucketer context
		bucketerContext.update(
				context.timestamp(),
				context.currentWatermark(),
				currentProcessingTime);

		final BucketID bucketId = bucketer.getBucketId(value, bucketerContext);

		Bucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
		if (bucket == null) {
			final Path bucketPath = assembleBucketPath(bucketId);
			bucket = bucketFactory.getNewBucket(
					fileSystemWriter,
					subtaskIndex,
					bucketId,
					bucketPath,
					maxPartCounterUsed,
					partFileWriterFactory);
			activeBuckets.put(bucketId, bucket);
		}

		final PartFileInfo<BucketID> info = bucket.getInProgressPartInfo();
		if (info == null || rollingPolicy.shouldRollOnEvent(info, value)) {

			// info will be null if there is no currently open part file. This
			// is the case when we have a new, just created bucket or a bucket
			// that has not received any data after the closing of its previously
			// open in-progress file due to the specified rolling policy.

			bucket.rollPartFile(currentProcessingTime);
		}
		bucket.write(value, currentProcessingTime);

		// we update the counter here because as buckets become inactive and
		// get removed in the initializeState(), at the time we snapshot they
		// may not be there to take them into account during checkpointing.
		updateMaxPartCounter(bucket.getPartCounter());
	}

	void onProcessingTime(long timestamp) throws Exception {
		for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
			final PartFileInfo<BucketID> info = bucket.getInProgressPartInfo();
			if (info != null && rollingPolicy.shouldRollOnProcessingTime(info, timestamp)) {
				bucket.closePartFile();
			}
		}
	}

	void close() {
		if (activeBuckets != null) {
			activeBuckets.values().forEach(Bucket::dispose);
		}
	}

	/**
	 * Assembles the final bucket {@link Path} that will be used for the provided bucket in the
	 * underlying filesystem.
	 * @param bucketId the id of the bucket as returned by the {@link Bucketer}.
	 * @return The resulting path.
	 */
	private Path assembleBucketPath(BucketID bucketId) {
		return new Path(basePath, bucketId.toString());
	}

	/**
	 * Updates the state keeping track of the maximum used part
	 * counter across all local active buckets.
	 * @param candidate the part counter that will potentially replace the current {@link #maxPartCounterUsed}.
	 */
	private void updateMaxPartCounter(long candidate) {
		maxPartCounterUsed = Math.max(maxPartCounterUsed, candidate);
	}

	/**
	 * The {@link Bucketer.Context} exposed to the
	 * {@link Bucketer#getBucketId(Object, Bucketer.Context)}
	 * whenever a new incoming element arrives.
	 */
	private static final class BucketerContext implements Bucketer.Context {

		@Nullable
		private Long elementTimestamp;

		private long currentWatermark;

		private long currentProcessingTime;

		private BucketerContext() {
			this.elementTimestamp = null;
			this.currentWatermark = Long.MIN_VALUE;
			this.currentProcessingTime = Long.MIN_VALUE;
		}

		void update(@Nullable Long element, long watermark, long processingTime) {
			this.elementTimestamp = element;
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
}
