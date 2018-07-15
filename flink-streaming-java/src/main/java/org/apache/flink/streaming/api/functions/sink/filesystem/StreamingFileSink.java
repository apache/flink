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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SimpleStringWriter;
import org.apache.flink.api.common.serialization.Writer;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.DateTimeBucketer;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Sink that emits its input elements to {@link FileSystem} files within buckets. This is
 * integrated with the checkpointing mechanism to provide exactly once semantics.
 *
 *
 * <p>When creating the sink a {@code basePath} must be specified. The base directory contains
 * one directory for every bucket. The bucket directories themselves contain several part files,
 * with at least one for each parallel subtask of the sink which is writing data to that bucket.
 * These part files contain the actual output data.
 *
 *
 * <p>The sink uses a {@link Bucketer} to determine in which bucket directory each element should
 * be written to inside the base directory. The {@code Bucketer} can, for example, use time or
 * a property of the element to determine the bucket directory. The default {@code Bucketer} is a
 * {@link DateTimeBucketer} which will create one new bucket every hour. You can specify
 * a custom {@code Bucketer} using {@link #setBucketer(Bucketer)}.
 *
 *
 * <p>The filenames of the part files contain the part prefix, "part-", the parallel subtask index of the sink
 * and a rolling counter. For example the file {@code "part-1-17"} contains the data from
 * {@code subtask 1} of the sink and is the {@code 17th} bucket created by that subtask.
 * Part files roll based on the user-specified {@link RollingPolicy}. By default, a {@link DefaultRollingPolicy}
 * is used.
 *
 * <p>In some scenarios, the open buckets are required to change based on time. In these cases, the user
 * can specify a {@code bucketCheckInterval} (by default 1m) and the sink will check periodically and roll
 * the part file if the specified rolling policy says so.
 *
 * <p>Part files can be in one of three states: {@code in-progress}, {@code pending} or {@code finished}.
 * The reason for this is how the sink works together with the checkpointing mechanism to provide exactly-once
 * semantics and fault-tolerance. The part file that is currently being written to is {@code in-progress}. Once
 * a part file is closed for writing it becomes {@code pending}. When a checkpoint is successful the currently
 * pending files will be moved to {@code finished}.
 *
 *
 * <p>If case of a failure, and in order to guarantee exactly-once semantics, the sink should roll back to the state it
 * had when that last successful checkpoint occurred. To this end, when restoring, the restored files in {@code pending}
 * state are transferred into the {@code finished} state while any {@code in-progress} files are rolled back, so that
 * they do not contain data that arrived after the checkpoint from which we restore.
 *
 * <p><b>NOTE:</b>
 * <ol>
 *     <li>
 *         If checkpointing is not enabled the pending files will never be moved to the finished state.
 *     </li>
 *     <li>
 *         The part files are written using an instance of {@link Writer}. By default, a
 *         {@link SimpleStringWriter} is used, which writes the result of {@code toString()} for
 *         every element, separated by newlines. You can configure the writer using the
 *         {@link #setWriter(Writer)}.
 *     </li>
 * </ol>
 *
 * @param <IN> Type of the elements emitted by this sink
 */
@PublicEvolving
public class StreamingFileSink<IN>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {

	private static final long serialVersionUID = 2544039385174378235L;

	private static final Logger LOG = LoggerFactory.getLogger(StreamingFileSink.class);

	private final Path basePath;

	private final BucketFactory<IN> bucketFactory;

	private long bucketCheckInterval = 60L * 1000L;

	private Bucketer<IN> bucketer;

	private Writer<IN> writer;

	private RollingPolicy rollingPolicy;

	private transient ResumableWriter fileSystemWriter;

	private transient ProcessingTimeService processingTimeService;

	private transient Map<String, Bucket<IN>> activeBuckets;

	//////////////////			State Related Fields			/////////////////////

	private transient BucketStateSerializer bucketStateSerializer;

	private final ListStateDescriptor<byte[]> bucketStateDesc =
			new ListStateDescriptor<>("bucket-states",
					BytePrimitiveArraySerializer.INSTANCE);

	private transient ListState<byte[]> restoredBucketStates;

	private final ListStateDescriptor<Long> maxPartCounterStateDesc =
			new ListStateDescriptor<>("max-part-counter",
					LongSerializer.INSTANCE);

	private transient ListState<Long> restoredMaxCounters;

	private long initMaxPartCounter;

	private long maxPartCounterUsed;

	/**
	 * Creates a new {@code StreamingFileSink} that writes files to the given base directory.
	 *
	 * <p>This uses a {@link DateTimeBucketer} as {@link Bucketer} and a {@link SimpleStringWriter} as a writer.
	 *
	 * @param basePath The directory to which to write the bucket files.
	 */
	public StreamingFileSink(Path basePath) {
		this(basePath, new DefaultBucketFactory<>());
	}

	@VisibleForTesting
	StreamingFileSink(Path basePath, BucketFactory<IN> bucketFactory) {
		this.basePath = Preconditions.checkNotNull(basePath);
		this.bucketer = new DateTimeBucketer<>();
		this.writer = new SimpleStringWriter<>();
		this.rollingPolicy = new DefaultRollingPolicy();
		this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
	}

	public StreamingFileSink<IN> setWriter(Writer<IN> writer) {
		this.writer = Preconditions.checkNotNull(writer);
		return this;
	}

	public StreamingFileSink<IN> setBucketer(Bucketer<IN> bucketer) {
		this.bucketer = Preconditions.checkNotNull(bucketer);
		return this;
	}

	public StreamingFileSink<IN> setBucketCheckInterval(long interval) {
		this.bucketCheckInterval = interval;
		return this;
	}

	public StreamingFileSink<IN> setRollingPolicy(RollingPolicy policy) {
		this.rollingPolicy = Preconditions.checkNotNull(policy);
		return this;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		final Iterator<Map.Entry<String, Bucket<IN>>> activeBucketIt =
				activeBuckets.entrySet().iterator();

		while (activeBucketIt.hasNext()) {
			Bucket<IN> bucket = activeBucketIt.next().getValue();
			bucket.commitUpToCheckpoint(checkpointId);

			if (!bucket.isActive()) {
				// We've dealt with all the pending files and the writer for this bucket is not currently open.
				// Therefore this bucket is currently inactive and we can remove it from our state.
				activeBucketIt.remove();
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkNotNull(restoredBucketStates);
		Preconditions.checkNotNull(fileSystemWriter);
		Preconditions.checkNotNull(bucketStateSerializer);

		restoredBucketStates.clear();
		for (Bucket<IN> bucket : activeBuckets.values()) {

			if (rollingPolicy.shouldRoll(bucket.getCurrentPartFileInfo(), context.getCheckpointTimestamp())) {
				// we also check here so that we do not have to always
				// wait for the "next" element to arrive.
				bucket.closePartFile();
			}

			final BucketState bucketState = bucket.snapshot(context.getCheckpointId());
			restoredBucketStates.add(bucketStateSerializer.serialize(bucketState));
		}

		restoredMaxCounters.clear();
		restoredMaxCounters.add(maxPartCounterUsed);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		initFileSystemWriter();

		this.activeBuckets = new HashMap<>();

		// When resuming after a failure:
		// 1) we get the max part counter used before in order to make sure that we do not overwrite valid data
		// 2) we commit any pending files for previous checkpoints (previous to the last successful one)
		// 3) we resume writing to the previous in-progress file of each bucket, and
		// 4) if we receive multiple states for the same bucket, we merge them.

		final OperatorStateStore stateStore = context.getOperatorStateStore();

		restoredBucketStates = stateStore.getListState(bucketStateDesc);
		restoredMaxCounters = stateStore.getUnionListState(maxPartCounterStateDesc);

		if (context.isRestored()) {
			final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

			LOG.info("Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIndex);

			for (long partCounter: restoredMaxCounters.get()) {
				if (partCounter > initMaxPartCounter) {
					initMaxPartCounter = partCounter;
				}
			}

			for (byte[] recoveredState : restoredBucketStates.get()) {
				final int version = bucketStateSerializer.getDeserializedVersion(recoveredState);
				final BucketState bucketState = bucketStateSerializer.deserialize(version, recoveredState);

				final String bucketId = bucketState.getBucketId();

				LOG.info("Recovered bucket for {}", bucketId);

				final Bucket<IN> restoredBucket = bucketFactory.getBucket(
						fileSystemWriter,
						subtaskIndex,
						initMaxPartCounter,
						writer,
						bucketState
				);

				final Bucket<IN> existingBucket = activeBuckets.get(bucketId);
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
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
		processingTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
		for (Bucket<IN> bucket : activeBuckets.values()) {
			if (rollingPolicy.shouldRoll(bucket.getCurrentPartFileInfo(), currentProcessingTime)) {
				bucket.closePartFile();
			}
		}
		processingTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
	}

	@Override
	public void invoke(IN value, Context context) throws Exception {
		final long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
		final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

		final String bucketId = bucketer.getBucketId(value, context);

		Bucket<IN> bucket = activeBuckets.get(bucketId);
		if (bucket == null) {
			final Path bucketPath = assembleBucketPath(bucketId);
			bucket = bucketFactory.getBucket(
					fileSystemWriter,
					subtaskIndex,
					bucketId,
					bucketPath,
					initMaxPartCounter,
					writer);
			activeBuckets.put(bucketId, bucket);
		}

		if (rollingPolicy.shouldRoll(bucket.getCurrentPartFileInfo(), currentProcessingTime)) {
			bucket.rollPartPartFile(currentProcessingTime);
		}
		bucket.write(value, currentProcessingTime);

		// we update the counter here because as buckets become inactive and
		// get removed in the initializeState(), at the time we snapshot they
		// may not be there to take them into account during checkpointing.
		updateMaxPartCounter(bucket.getPartCounter());
	}

	@Override
	public void close() throws Exception {
		if (activeBuckets != null) {
			// here we cannot "commit" because this is also called in case of failures.
			for (Bucket<IN> bucket : activeBuckets.values()) {
				bucket.closePartFile();
			}
		}
	}

	private void initFileSystemWriter() throws IOException {
		if (fileSystemWriter == null) {
			fileSystemWriter = FileSystem.get(basePath.toUri()).createRecoverableWriter();
			bucketStateSerializer = new BucketStateSerializer(
					fileSystemWriter.getResumeRecoverableSerializer(),
					fileSystemWriter.getCommitRecoverableSerializer()
			);
		}
	}

	private void updateMaxPartCounter(long candidate) {
		if (candidate > maxPartCounterUsed) {
			this.maxPartCounterUsed = candidate;
		}
	}

	private Path assembleBucketPath(String bucketId) {
		return new Path(basePath, bucketId);
	}
}
