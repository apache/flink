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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
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
 * <p>The sink uses a {@link BucketAssigner} to determine in which bucket directory each element should
 * be written to inside the base directory. The {@code BucketAssigner} can, for example, use time or
 * a property of the element to determine the bucket directory. The default {@code BucketAssigner} is a
 * {@link DateTimeBucketAssigner} which will create one new bucket every hour. You can specify
 * a custom {@code BucketAssigner} using the {@code setBucketAssigner(bucketAssigner)} method, after calling
 * {@link StreamingFileSink#forRowFormat(Path, Encoder)} or
 * {@link StreamingFileSink#forBulkFormat(Path, BulkWriter.Factory)}.
 *
 *
 * <p>The filenames of the part files could be defined using {@link OutputFileConfig}, this configuration contain
 * a part prefix and part suffix, that will be used with the parallel subtask index of the sink
 * and a rolling counter. For example for a prefix "prefix" and a suffix ".ext" the file create will have a name
 * {@code "prefix-1-17.ext"} containing the data from {@code subtask 1} of the sink and is the {@code 17th} bucket
 * created by that subtask.
 * Part files roll based on the user-specified {@link RollingPolicy}. By default, a {@link DefaultRollingPolicy}
 * is used for row-encoded sink output; a {@link OnCheckpointRollingPolicy} is used for bulk-encoded sink output.
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
		implements CheckpointedFunction, CheckpointListener {

	private static final long serialVersionUID = 1L;

	// ------------------------ configuration fields --------------------------

	private final long bucketCheckInterval;

	private final BucketsBuilder<IN, ?, ? extends BucketsBuilder<IN, ?, ?>> bucketsBuilder;

	// --------------------------- runtime fields -----------------------------

	private transient StreamingFileSinkHelper<IN> helper;

	/**
	 * Creates a new {@code StreamingFileSink} that writes files to the given base directory
	 * with the give buckets properties.
	 */
	protected StreamingFileSink(
		BucketsBuilder<IN, ?, ? extends BucketsBuilder<IN, ?, ?>> bucketsBuilder,
		long bucketCheckInterval) {

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
	public static <IN> StreamingFileSink.DefaultRowFormatBuilder<IN> forRowFormat(
			final Path basePath, final Encoder<IN> encoder) {
		return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
	}

	/**
	 * Creates the builder for a {@link StreamingFileSink} with row-encoding format.
	 * @param basePath the base path where all the buckets are going to be created as sub-directories.
	 * @param writerFactory the {@link BulkWriter.Factory} to be used when writing elements in the buckets.
	 * @param <IN> the type of incoming elements
	 * @return The builder where the remaining of the configuration parameters for the sink can be configured.
	 * In order to instantiate the sink, call {@link RowFormatBuilder#build()} after specifying the desired parameters.
	 */
	public static <IN> StreamingFileSink.DefaultBulkFormatBuilder<IN> forBulkFormat(
			final Path basePath, final BulkWriter.Factory<IN> writerFactory) {
		return new StreamingFileSink.DefaultBulkFormatBuilder<>(basePath, writerFactory, new DateTimeBucketAssigner<>());
	}

	/**
	 * The base abstract class for the {@link RowFormatBuilder} and {@link BulkFormatBuilder}.
	 */
	@Internal
	public abstract static class BucketsBuilder<IN, BucketID, T extends BucketsBuilder<IN, BucketID, T>> implements Serializable {

		private static final long serialVersionUID = 1L;

		public static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

		@SuppressWarnings("unchecked")
		protected T self() {
			return (T) this;
		}

		@Internal
		public abstract Buckets<IN, BucketID> createBuckets(final int subtaskIndex) throws IOException;
	}

	/**
	 * A builder for configuring the sink for row-wise encoding formats.
	 */
	@PublicEvolving
	public static class RowFormatBuilder<IN, BucketID, T extends RowFormatBuilder<IN, BucketID, T>> extends StreamingFileSink.BucketsBuilder<IN, BucketID, T> {

		private static final long serialVersionUID = 1L;

		private long bucketCheckInterval;

		private final Path basePath;

		private Encoder<IN> encoder;

		private BucketAssigner<IN, BucketID> bucketAssigner;

		private RollingPolicy<IN, BucketID> rollingPolicy;

		private BucketFactory<IN, BucketID> bucketFactory;

		private OutputFileConfig outputFileConfig;

		protected RowFormatBuilder(Path basePath, Encoder<IN> encoder, BucketAssigner<IN, BucketID> bucketAssigner) {
			this(basePath, encoder, bucketAssigner, DefaultRollingPolicy.builder().build(), DEFAULT_BUCKET_CHECK_INTERVAL, new DefaultBucketFactoryImpl<>(), OutputFileConfig.builder().build());
		}

		protected RowFormatBuilder(
				Path basePath,
				Encoder<IN> encoder,
				BucketAssigner<IN, BucketID> assigner,
				RollingPolicy<IN, BucketID> policy,
				long bucketCheckInterval,
				BucketFactory<IN, BucketID> bucketFactory,
				OutputFileConfig outputFileConfig) {
			this.basePath = Preconditions.checkNotNull(basePath);
			this.encoder = Preconditions.checkNotNull(encoder);
			this.bucketAssigner = Preconditions.checkNotNull(assigner);
			this.rollingPolicy = Preconditions.checkNotNull(policy);
			this.bucketCheckInterval = bucketCheckInterval;
			this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
			this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
		}

		public long getBucketCheckInterval() {
			return bucketCheckInterval;
		}

		public T withBucketCheckInterval(final long interval) {
			this.bucketCheckInterval = interval;
			return self();
		}

		public T withBucketAssigner(final BucketAssigner<IN, BucketID> assigner) {
			this.bucketAssigner = Preconditions.checkNotNull(assigner);
			return self();
		}

		public T withRollingPolicy(final RollingPolicy<IN, BucketID> policy) {
			this.rollingPolicy = Preconditions.checkNotNull(policy);
			return self();
		}

		public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
			this.outputFileConfig = outputFileConfig;
			return self();
		}

		public <ID> StreamingFileSink.RowFormatBuilder<IN, ID, ? extends RowFormatBuilder<IN, ID, ?>> withNewBucketAssignerAndPolicy(final BucketAssigner<IN, ID> assigner, final RollingPolicy<IN, ID> policy) {
			Preconditions.checkState(bucketFactory.getClass() == DefaultBucketFactoryImpl.class, "newBuilderWithBucketAssignerAndPolicy() cannot be called after specifying a customized bucket factory");
			return new RowFormatBuilder(basePath, encoder, Preconditions.checkNotNull(assigner), Preconditions.checkNotNull(policy), bucketCheckInterval, new DefaultBucketFactoryImpl<>(), outputFileConfig);
		}

		/** Creates the actual sink. */
		public StreamingFileSink<IN> build() {
			return new StreamingFileSink<>(this, bucketCheckInterval);
		}

		@VisibleForTesting
		T withBucketFactory(final BucketFactory<IN, BucketID> factory) {
			this.bucketFactory = Preconditions.checkNotNull(factory);
			return self();
		}

		@Internal
		@Override
		public Buckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
			return new Buckets<>(
					basePath,
					bucketAssigner,
					bucketFactory,
					new RowWiseBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder),
					rollingPolicy,
					subtaskIndex,
					outputFileConfig);
		}
	}

	/**
	 * Builder for the vanilla {@link StreamingFileSink} using a row format.
	 * @param <IN> record type
	 */
	public static final class DefaultRowFormatBuilder<IN> extends RowFormatBuilder<IN, String, DefaultRowFormatBuilder<IN>> {
		private static final long serialVersionUID = -8503344257202146718L;

		private DefaultRowFormatBuilder(Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
			super(basePath, encoder, bucketAssigner);
		}
	}

	/**
	 * A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC.
	 */
	@PublicEvolving
	public static class BulkFormatBuilder<IN, BucketID, T extends BulkFormatBuilder<IN, BucketID, T>> extends StreamingFileSink.BucketsBuilder<IN, BucketID, T> {

		private static final long serialVersionUID = 1L;

		private long bucketCheckInterval;

		private final Path basePath;

		private BulkWriter.Factory<IN> writerFactory;

		private BucketAssigner<IN, BucketID> bucketAssigner;

		private CheckpointRollingPolicy<IN, BucketID> rollingPolicy;

		private BucketFactory<IN, BucketID> bucketFactory;

		private OutputFileConfig outputFileConfig;

		protected BulkFormatBuilder(Path basePath, BulkWriter.Factory<IN> writerFactory, BucketAssigner<IN, BucketID> assigner) {
			this(basePath, writerFactory, assigner, OnCheckpointRollingPolicy.build(), DEFAULT_BUCKET_CHECK_INTERVAL,
				new DefaultBucketFactoryImpl<>(), OutputFileConfig.builder().build());
		}

		protected BulkFormatBuilder(
				Path basePath,
				BulkWriter.Factory<IN> writerFactory,
				BucketAssigner<IN, BucketID> assigner,
				CheckpointRollingPolicy<IN, BucketID> policy,
				long bucketCheckInterval,
				BucketFactory<IN, BucketID> bucketFactory,
				OutputFileConfig outputFileConfig) {
			this.basePath = Preconditions.checkNotNull(basePath);
			this.writerFactory = writerFactory;
			this.bucketAssigner = Preconditions.checkNotNull(assigner);
			this.rollingPolicy = Preconditions.checkNotNull(policy);
			this.bucketCheckInterval = bucketCheckInterval;
			this.bucketFactory = Preconditions.checkNotNull(bucketFactory);
			this.outputFileConfig = Preconditions.checkNotNull(outputFileConfig);
		}

		public long getBucketCheckInterval() {
			return bucketCheckInterval;
		}

		public T withBucketCheckInterval(long interval) {
			this.bucketCheckInterval = interval;
			return self();
		}

		public T withBucketAssigner(BucketAssigner<IN, BucketID> assigner) {
			this.bucketAssigner = Preconditions.checkNotNull(assigner);
			return self();
		}

		public T withRollingPolicy(CheckpointRollingPolicy<IN, BucketID> rollingPolicy) {
			this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
			return self();
		}

		@VisibleForTesting
		T withBucketFactory(final BucketFactory<IN, BucketID> factory) {
			this.bucketFactory = Preconditions.checkNotNull(factory);
			return self();
		}

		public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
			this.outputFileConfig = outputFileConfig;
			return self();
		}

		public <ID> StreamingFileSink.BulkFormatBuilder<IN, ID, ? extends BulkFormatBuilder<IN, ID, ?>> withNewBucketAssigner(final BucketAssigner<IN, ID> assigner) {
			Preconditions.checkState(bucketFactory.getClass() == DefaultBucketFactoryImpl.class, "newBuilderWithBucketAssigner() cannot be called after specifying a customized bucket factory");
			return new BulkFormatBuilder(basePath, writerFactory, Preconditions.checkNotNull(assigner),
				rollingPolicy, bucketCheckInterval, new DefaultBucketFactoryImpl<>(), outputFileConfig);
		}

		/** Creates the actual sink. */
		public StreamingFileSink<IN> build() {
			return new StreamingFileSink<>(this, bucketCheckInterval);
		}

		@Internal
		@Override
		public Buckets<IN, BucketID> createBuckets(int subtaskIndex) throws IOException {
			return new Buckets<>(
					basePath,
					bucketAssigner,
					bucketFactory,
					new BulkBucketWriter<>(FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory),
					rollingPolicy,
					subtaskIndex,
					outputFileConfig);
		}
	}

	/**
	 * Builder for the vanilla {@link StreamingFileSink} using a bulk format.
	 * @param <IN> record type
	 */
	public static final class DefaultBulkFormatBuilder<IN> extends BulkFormatBuilder<IN, String, DefaultBulkFormatBuilder<IN>> {

		private static final long serialVersionUID = 7493169281036370228L;

		private DefaultBulkFormatBuilder(Path basePath, BulkWriter.Factory<IN> writerFactory, BucketAssigner<IN, String> assigner) {
			super(basePath, writerFactory, assigner);
		}
	}

	// --------------------------- Sink Methods -----------------------------

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.helper = new StreamingFileSinkHelper<>(
				bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask()),
				context.isRestored(),
				context.getOperatorStateStore(),
				((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService(),
				bucketCheckInterval);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		this.helper.commitUpToCheckpoint(checkpointId);
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(helper != null, "sink has not been initialized");
		this.helper.snapshotState(context.getCheckpointId());
	}

	@Override
	public void invoke(IN value, SinkFunction.Context context) throws Exception {
		this.helper.onElement(
				value,
				context.currentProcessingTime(),
				context.timestamp(),
				context.currentWatermark());
	}

	@Override
	public void close() throws Exception {
		if (this.helper != null) {
			this.helper.close();
		}
	}
}
