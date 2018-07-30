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
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.DateTimeBucketer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rolling.policies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rolling.policies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

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
 * a custom {@code Bucketer} using the {@code setBucketer(Bucketer)} method, after calling
 * {@link StreamingFileSink#forRowFormat(Path, Encoder)} or
 * {@link StreamingFileSink#forBulkFormat(Path, BulkWriter.Factory)}.
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
 * @param <IN> Type of the elements emitted by this sink
 */
@PublicEvolving
public class StreamingFileSink<IN>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	// -------------------------- state descriptors ---------------------------

	private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
			new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

	private static final ListStateDescriptor<Long> MAX_PART_COUNTER_STATE_DESC =
			new ListStateDescriptor<>("max-part-counter", LongSerializer.INSTANCE);

	// ------------------------ configuration fields --------------------------

	private final long bucketCheckInterval;

	private final StreamingFileSink.BucketsBuilder<IN, ?> bucketsBuilder;

	// --------------------------- runtime fields -----------------------------

	private transient Buckets<IN, ?> buckets;

	private transient ProcessingTimeService processingTimeService;

	// --------------------------- State Related Fields -----------------------------

	private transient ListState<byte[]> bucketStates;

	private transient ListState<Long> maxPartCountersState;

	/**
	 * Creates a new {@code StreamingFileSink} that writes files to the given base directory.
	 */
	private StreamingFileSink(
			final StreamingFileSink.BucketsBuilder<IN, ?> bucketsBuilder,
			final long bucketCheckInterval) {

		Preconditions.checkArgument(bucketCheckInterval > 0L);

		this.bucketsBuilder = Preconditions.checkNotNull(bucketsBuilder);
		this.bucketCheckInterval = bucketCheckInterval;
	}

	// ------------------------------------------------------------------------

	// --------------------------- Sink Builders  -----------------------------

	/**
	 * Creates the builder for a {@code StreamingFileSink} with row-encoding format.
	 * @param basePath the base path where all the buckets are going to be created as sub-directories.
	 * @param encoder the {@link Encoder} to be used when writing elements in the buckets.
	 * @param <IN> the type of incoming elements
	 * @return The builder where the remaining of the configuration parameters for the sink can be configured.
	 * In order to instantiate the sink, call {@link RowFormatBuilder#build()} after specifying the desired parameters.
	 */
	public static <IN> StreamingFileSink.RowFormatBuilder<IN, String> forRowFormat(
			final Path basePath, final Encoder<IN> encoder) {
		return new StreamingFileSink.RowFormatBuilder<>(basePath, encoder, new DateTimeBucketer<>());
	}

	/**
	 * Creates the builder for a {@link StreamingFileSink} with row-encoding format.
	 * @param basePath the base path where all the buckets are going to be created as sub-directories.
	 * @param writerFactory the {@link BulkWriter.Factory} to be used when writing elements in the buckets.
	 * @param <IN> the type of incoming elements
	 * @return The builder where the remaining of the configuration parameters for the sink can be configured.
	 * In order to instantiate the sink, call {@link RowFormatBuilder#build()} after specifying the desired parameters.
	 */
	public static <IN> StreamingFileSink.BulkFormatBuilder<IN, String> forBulkFormat(
			final Path basePath, final BulkWriter.Factory<IN> writerFactory) {
		return new StreamingFileSink.BulkFormatBuilder<>(basePath, writerFactory, new DateTimeBucketer<>());
	}

	/**
	 * The base abstract class for the {@link RowFormatBuilder} and {@link BulkFormatBuilder}.
	 */
	private abstract static class BucketsBuilder<IN, BucketID> implements Serializable {

		private static final long serialVersionUID = 1L;

		abstract Buckets<IN, BucketID> createBuckets(final int subtaskIndex) throws IOException;
	}

	/**
	 * A builder for configuring the sink for row-wise encoding formats.
	 */
	@PublicEvolving
	public static class RowFormatBuilder<IN, BucketID> extends StreamingFileSink.BucketsBuilder<IN, BucketID> {

		private static final long serialVersionUID = 1L;

		private final long bucketCheckInterval;

		private final Path basePath;

		private final Encoder<IN> encoder;

		private final Bucketer<IN, BucketID> bucketer;

		private final RollingPolicy<IN, BucketID> rollingPolicy;

		private final BucketFactory<IN, BucketID> bucketFactory;

		RowFormatBuilder(Path basePath, Encoder<IN> encoder, Bucketer<IN, BucketID> bucketer) {
			this(basePath, encoder, bucketer, DefaultRollingPolicy.create().build(), 60L * 1000L, new DefaultBucketFactory<>());
		}

		private RowFormatBuilder(
				Path basePath,
				Encoder<IN> encoder,
				Bucketer<IN, BucketID> bucketer,
				RollingPolicy<IN, BucketID> rollingPolicy,
				long bucketCheckInterval,
				BucketFactory<IN, BucketID> bucketFactory) {
			this.basePath = Preconditions.checkNotNull(basePath);
			this.encoder = Preconditions.checkNotNull(encoder);
			this.bucketer = Preconditions.checkNotNull(bucketer);
			this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
			this.bucketCheckInterval = bucketCheckInterval;
			this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
		}

		public StreamingFileSink.RowFormatBuilder<IN, BucketID> withBucketCheckInterval(final long interval) {
			return new RowFormatBuilder<>(basePath, encoder, bucketer, rollingPolicy, interval, bucketFactory);
		}

		public StreamingFileSink.RowFormatBuilder<IN, BucketID> withBucketer(final Bucketer<IN, BucketID> bucketer) {
			return new RowFormatBuilder<>(basePath, encoder, Preconditions.checkNotNull(bucketer), rollingPolicy, bucketCheckInterval, bucketFactory);
		}

		public StreamingFileSink.RowFormatBuilder<IN, BucketID> withRollingPolicy(final RollingPolicy<IN, BucketID> policy) {
			return new RowFormatBuilder<>(basePath, encoder, bucketer, Preconditions.checkNotNull(policy), bucketCheckInterval, bucketFactory);
		}

		public <ID> StreamingFileSink.RowFormatBuilder<IN, ID> withBucketerAndPolicy(final Bucketer<IN, ID> bucketer, final RollingPolicy<IN, ID> policy) {
			return new RowFormatBuilder<>(basePath, encoder, Preconditions.checkNotNull(bucketer), Preconditions.checkNotNull(policy), bucketCheckInterval, new DefaultBucketFactory<>());
		}

		@VisibleForTesting
		StreamingFileSink.RowFormatBuilder<IN, BucketID> withBucketFactory(final BucketFactory<IN, BucketID> factory) {
			return new RowFormatBuilder<>(basePath, encoder, bucketer, rollingPolicy, bucketCheckInterval, Preconditions.checkNotNull(factory));
		}

		/** Creates the actual sink. */
		public StreamingFileSink<IN> build() {
			return new StreamingFileSink<>(this, bucketCheckInterval);
		}

		@Override
		Buckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
			return new Buckets<>(
					basePath,
					bucketer,
					bucketFactory,
					new RowWisePartWriter.Factory<>(encoder),
					rollingPolicy,
					subtaskIndex);
		}
	}

	/**
	 * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC.
	 */
	@PublicEvolving
	public static class BulkFormatBuilder<IN, BucketID> extends StreamingFileSink.BucketsBuilder<IN, BucketID> {

		private static final long serialVersionUID = 1L;

		private final long bucketCheckInterval;

		private final Path basePath;

		private final BulkWriter.Factory<IN> writerFactory;

		private final Bucketer<IN, BucketID> bucketer;

		private final BucketFactory<IN, BucketID> bucketFactory;

		BulkFormatBuilder(Path basePath, BulkWriter.Factory<IN> writerFactory, Bucketer<IN, BucketID> bucketer) {
			this(basePath, writerFactory, bucketer, 60L * 1000L, new DefaultBucketFactory<>());
		}

		private BulkFormatBuilder(
				Path basePath,
				BulkWriter.Factory<IN> writerFactory,
				Bucketer<IN, BucketID> bucketer,
				long bucketCheckInterval,
				BucketFactory<IN, BucketID> bucketFactory) {
			this.basePath = Preconditions.checkNotNull(basePath);
			this.writerFactory = writerFactory;
			this.bucketer = Preconditions.checkNotNull(bucketer);
			this.bucketCheckInterval = bucketCheckInterval;
			this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
		}

		public StreamingFileSink.BulkFormatBuilder<IN, BucketID> withBucketCheckInterval(long interval) {
			return new BulkFormatBuilder<>(basePath, writerFactory, bucketer, interval, bucketFactory);
		}

		public <ID> StreamingFileSink.BulkFormatBuilder<IN, ID> withBucketer(Bucketer<IN, ID> bucketer) {
			return new BulkFormatBuilder<>(basePath, writerFactory, Preconditions.checkNotNull(bucketer), bucketCheckInterval, new DefaultBucketFactory<>());
		}

		@VisibleForTesting
		StreamingFileSink.BulkFormatBuilder<IN, BucketID> withBucketFactory(final BucketFactory<IN, BucketID> factory) {
			return new BulkFormatBuilder<>(basePath, writerFactory, bucketer, bucketCheckInterval, Preconditions.checkNotNull(factory));
		}

		/** Creates the actual sink. */
		public StreamingFileSink<IN> build() {
			return new StreamingFileSink<>(this, bucketCheckInterval);
		}

		@Override
		Buckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
			return new Buckets<>(
					basePath,
					bucketer,
					bucketFactory,
					new BulkPartWriter.Factory<>(writerFactory),
					new OnCheckpointRollingPolicy<>(),
					subtaskIndex);
		}
	}

	// --------------------------- Sink Methods -----------------------------

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		this.buckets = bucketsBuilder.createBuckets(subtaskIndex);

		final OperatorStateStore stateStore = context.getOperatorStateStore();
		bucketStates = stateStore.getListState(BUCKET_STATE_DESC);
		maxPartCountersState = stateStore.getUnionListState(MAX_PART_COUNTER_STATE_DESC);

		if (context.isRestored()) {
			buckets.initializeState(bucketStates, maxPartCountersState);
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		buckets.publishUpToCheckpoint(checkpointId);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(bucketStates != null && maxPartCountersState != null, "sink has not been initialized");

		bucketStates.clear();
		maxPartCountersState.clear();

		buckets.snapshotState(
				context.getCheckpointId(),
				bucketStates,
				maxPartCountersState);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
		long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
		processingTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		final long currentTime = processingTimeService.getCurrentProcessingTime();
		buckets.onProcessingTime(currentTime);
		processingTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}

	@Override
	public void invoke(IN value, SinkFunction.Context context) throws Exception {
		buckets.onElement(value, context);
	}

	@Override
	public void close() throws Exception {
		buckets.close();
	}
}
