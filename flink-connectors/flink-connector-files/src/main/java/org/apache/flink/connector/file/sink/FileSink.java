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

package org.apache.flink.connector.file.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.StatefulSink.WithCompatibleState;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.connector.file.sink.committer.FileCommitter;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinatorFactory;
import org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinatorStateHandlerFactory;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperatorFactory;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperatorStateHandlerFactory;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequest;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequestTypeInfo;
import org.apache.flink.connector.file.sink.writer.DefaultFileWriterBucketFactory;
import org.apache.flink.connector.file.sink.writer.FileWriter;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketFactory;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketState;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketStateSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.RowWiseBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.types.Either;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A unified sink that emits its input elements to {@link FileSystem} files within buckets. This
 * sink achieves exactly-once semantics for both {@code BATCH} and {@code STREAMING}.
 *
 * <p>When creating the sink a {@code basePath} must be specified. The base directory contains one
 * directory for every bucket. The bucket directories themselves contain several part files, with at
 * least one for each parallel subtask of the sink which is writing data to that bucket. These part
 * files contain the actual output data.
 *
 * <p>The sink uses a {@link BucketAssigner} to determine in which bucket directory each element
 * should be written to inside the base directory. The {@code BucketAssigner} can, for example, roll
 * on every checkpoint or use time or a property of the element to determine the bucket directory.
 * The default {@code BucketAssigner} is a {@link DateTimeBucketAssigner} which will create one new
 * bucket every hour. You can specify a custom {@code BucketAssigner} using the {@code
 * setBucketAssigner(bucketAssigner)} method, after calling {@link FileSink#forRowFormat(Path,
 * Encoder)} or {@link FileSink#forBulkFormat(Path, BulkWriter.Factory)}.
 *
 * <p>The names of the part files could be defined using {@link OutputFileConfig}. This
 * configuration contains a part prefix and a part suffix that will be used with a random uid
 * assigned to each subtask of the sink and a rolling counter to determine the file names. For
 * example with a prefix "prefix" and a suffix ".ext", a file named {@code
 * "prefix-81fc4980-a6af-41c8-9937-9939408a734b-17.ext"} contains the data from subtask with uid
 * {@code 81fc4980-a6af-41c8-9937-9939408a734b} of the sink and is the {@code 17th} part-file
 * created by that subtask.
 *
 * <p>Part files roll based on the user-specified {@link RollingPolicy}. By default, a {@link
 * DefaultRollingPolicy} is used for row-encoded sink output; a {@link OnCheckpointRollingPolicy} is
 * used for bulk-encoded sink output.
 *
 * <p>In some scenarios, the open buckets are required to change based on time. In these cases, the
 * user can specify a {@code bucketCheckInterval} (by default 1m) and the sink will check
 * periodically and roll the part file if the specified rolling policy says so.
 *
 * <p>Part files can be in one of three states: {@code in-progress}, {@code pending} or {@code
 * finished}. The reason for this is how the sink works to provide exactly-once semantics and
 * fault-tolerance. The part file that is currently being written to is {@code in-progress}. Once a
 * part file is closed for writing it becomes {@code pending}. When a checkpoint is successful (for
 * {@code STREAMING}) or at the end of the job (for {@code BATCH}) the currently pending files will
 * be moved to {@code finished}.
 *
 * <p>For {@code STREAMING} in order to guarantee exactly-once semantics in case of a failure, the
 * sink should roll back to the state it had when that last successful checkpoint occurred. To this
 * end, when restoring, the restored files in {@code pending} state are transferred into the {@code
 * finished} state while any {@code in-progress} files are rolled back, so that they do not contain
 * data that arrived after the checkpoint from which we restore.
 *
 * <p>FileSink also support compacting small files to accelerate the access speed of the resulted
 * files. Compaction could be enabled via {@code enableCompact}. Once enabled, the compaction could
 * only be disabled via calling {@code disableCompact} explicitly, otherwise there might be data
 * loss.
 *
 * @param <IN> Type of the elements in the input of the sink that are also the elements to be
 *     written to its output
 */
@Experimental
public class FileSink<IN>
        implements StatefulSink<IN, FileWriterBucketState>,
                TwoPhaseCommittingSink<IN, FileSinkCommittable>,
                WithCompatibleState,
                WithPreCommitTopology<IN, FileSinkCommittable>,
                SupportsConcurrentExecutionAttempts {

    private final BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder;

    private FileSink(BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder) {
        this.bucketsBuilder = checkNotNull(bucketsBuilder);
    }

    @Override
    public FileWriter<IN> createWriter(InitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public FileWriter<IN> restoreWriter(
            InitContext context, Collection<FileWriterBucketState> recoveredState)
            throws IOException {
        FileWriter<IN> writer = bucketsBuilder.createWriter(context);
        writer.initializeState(recoveredState);
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer() {
        try {
            return bucketsBuilder.getWriterStateSerializer();
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create writer state serializer.", e);
        }
    }

    @Override
    public Committer<FileSinkCommittable> createCommitter() throws IOException {
        return bucketsBuilder.createCommitter();
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        try {
            return bucketsBuilder.getCommittableSerializer();
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public Collection<String> getCompatibleWriterStateNames() {
        // StreamingFileSink
        return Collections.singleton("bucket-states");
    }

    public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
            final Path basePath, final Encoder<IN> encoder) {
        return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
    }

    public static <IN> DefaultBulkFormatBuilder<IN> forBulkFormat(
            final Path basePath, final BulkWriter.Factory<IN> bulkWriterFactory) {
        return new DefaultBulkFormatBuilder<>(
                basePath, bulkWriterFactory, new DateTimeBucketAssigner<>());
    }

    @Override
    public DataStream<CommittableMessage<FileSinkCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<FileSinkCommittable>> committableStream) {
        FileCompactStrategy strategy = bucketsBuilder.getCompactStrategy();
        if (strategy == null && !bucketsBuilder.isCompactDisabledExplicitly()) {
            // compact is never enabled, we may not add the handlers
            return committableStream;
        }

        if (strategy == null) {
            // not enabled at present, handlers will be added to process the remaining states of the
            // compact coordinator and the compactor operators.
            SingleOutputStreamOperator<
                            Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>>
                    coordinatorOp =
                            committableStream
                                    .forward()
                                    .transform(
                                            "CompactorCoordinatorPlaceHolder",
                                            new EitherTypeInfo<>(
                                                    committableStream.getType(),
                                                    new CompactorRequestTypeInfo(
                                                            bucketsBuilder
                                                                    ::getCommittableSerializer)),
                                            new CompactCoordinatorStateHandlerFactory(
                                                    bucketsBuilder::getCommittableSerializer));
            coordinatorOp
                    .getTransformation()
                    .setParallelism(committableStream.getParallelism(), false);
            coordinatorOp.uid("FileSinkCompactorCoordinator");

            SingleOutputStreamOperator<CommittableMessage<FileSinkCommittable>> operator =
                    coordinatorOp
                            .forward()
                            .transform(
                                    "CompactorOperatorPlaceHolder",
                                    committableStream.getType(),
                                    new CompactorOperatorStateHandlerFactory(
                                            bucketsBuilder::getCommittableSerializer,
                                            bucketsBuilder::createBucketWriter));
            operator.getTransformation().setParallelism(committableStream.getParallelism(), false);
            return operator.uid("FileSinkCompactorOperator");
        }

        // explicitly rebalance here is required, or the partitioner will be forward, which is in
        // fact the partitioner from the writers to the committers
        SingleOutputStreamOperator<CompactorRequest> coordinatorOp =
                committableStream
                        .rebalance()
                        .transform(
                                "CompactorCoordinator",
                                new CompactorRequestTypeInfo(
                                        bucketsBuilder::getCommittableSerializer),
                                new CompactCoordinatorFactory(
                                        strategy, bucketsBuilder::getCommittableSerializer))
                        .setParallelism(1)
                        .uid("FileSinkCompactorCoordinator");

        // parallelism of the compactors is not configurable at present, since it must be identical
        // to that of the committers, or the committable summary and the committables may be
        // distributed to different committers, which will cause a failure
        TypeInformation<CommittableMessage<FileSinkCommittable>> committableType =
                committableStream.getType();
        SingleOutputStreamOperator<CommittableMessage<FileSinkCommittable>> operator =
                coordinatorOp.transform(
                        "CompactorOperator",
                        committableType,
                        new CompactorOperatorFactory(
                                strategy,
                                bucketsBuilder.getFileCompactor(),
                                bucketsBuilder::getCommittableSerializer,
                                bucketsBuilder::createBucketWriter));
        operator.getTransformation().setParallelism(committableStream.getParallelism(), false);
        return operator.uid("FileSinkCompactorOperator");
    }

    /** The base abstract class for the {@link RowFormatBuilder} and {@link BulkFormatBuilder}. */
    @Internal
    private abstract static class BucketsBuilder<IN, T extends BucketsBuilder<IN, T>>
            implements Serializable {

        private static final long serialVersionUID = 1L;

        protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        @Internal
        abstract FileWriter<IN> createWriter(final InitContext context) throws IOException;

        @Internal
        abstract FileCommitter createCommitter() throws IOException;

        @Internal
        abstract SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
                throws IOException;

        @Internal
        abstract SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
                throws IOException;

        @Internal
        abstract boolean isCompactDisabledExplicitly();

        @Internal
        abstract FileCompactStrategy getCompactStrategy();

        @Internal
        abstract FileCompactor getFileCompactor();

        @Internal
        abstract BucketWriter<IN, String> createBucketWriter() throws IOException;
    }

    /** A builder for configuring the sink for row-wise encoding formats. */
    public static class RowFormatBuilder<IN, T extends RowFormatBuilder<IN, T>>
            extends BucketsBuilder<IN, T> {

        private static final long serialVersionUID = 1L;

        private final Path basePath;

        private long bucketCheckInterval;

        private final Encoder<IN> encoder;

        private final FileWriterBucketFactory<IN> bucketFactory;

        private BucketAssigner<IN, String> bucketAssigner;

        private RollingPolicy<IN, String> rollingPolicy;

        private OutputFileConfig outputFileConfig;

        private boolean isCompactDisabledExplicitly = false;

        private FileCompactStrategy compactStrategy;

        private FileCompactor fileCompactor;

        protected RowFormatBuilder(
                Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
            this(
                    basePath,
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    encoder,
                    bucketAssigner,
                    DefaultRollingPolicy.builder().build(),
                    new DefaultFileWriterBucketFactory<>(),
                    OutputFileConfig.builder().build());
        }

        protected RowFormatBuilder(
                Path basePath,
                long bucketCheckInterval,
                Encoder<IN> encoder,
                BucketAssigner<IN, String> assigner,
                RollingPolicy<IN, String> policy,
                FileWriterBucketFactory<IN> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = checkNotNull(basePath);
            this.bucketCheckInterval = bucketCheckInterval;
            this.encoder = checkNotNull(encoder);
            this.bucketAssigner = checkNotNull(assigner);
            this.rollingPolicy = checkNotNull(policy);
            this.bucketFactory = checkNotNull(bucketFactory);
            this.outputFileConfig = checkNotNull(outputFileConfig);
        }

        public T withBucketCheckInterval(final long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(final BucketAssigner<IN, String> assigner) {
            this.bucketAssigner = checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(final RollingPolicy<IN, String> policy) {
            this.rollingPolicy = checkNotNull(policy);
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

        public T enableCompact(final FileCompactStrategy strategy, final FileCompactor compactor) {
            this.compactStrategy = checkNotNull(strategy);
            this.fileCompactor = checkNotNull(compactor);
            return self();
        }

        public T disableCompact() {
            this.isCompactDisabledExplicitly = true;
            return self();
        }

        /** Creates the actual sink. */
        public FileSink<IN> build() {
            return new FileSink<>(this);
        }

        @Override
        FileWriter<IN> createWriter(InitContext context) throws IOException {
            OutputFileConfig writerFileConfig;
            if (compactStrategy == null) {
                writerFileConfig = outputFileConfig;
            } else {
                // Compaction is enabled. We always commit before compacting, so the file written by
                // writer should be hid.
                writerFileConfig =
                        OutputFileConfig.builder()
                                .withPartPrefix("." + outputFileConfig.getPartPrefix())
                                .withPartSuffix(outputFileConfig.getPartSuffix())
                                .build();
            }

            return new FileWriter<>(
                    basePath,
                    context.metricGroup(),
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    writerFileConfig,
                    context.getProcessingTimeService(),
                    bucketCheckInterval);
        }

        @Override
        FileCommitter createCommitter() throws IOException {
            return new FileCommitter(createBucketWriter());
        }

        @Override
        boolean isCompactDisabledExplicitly() {
            return isCompactDisabledExplicitly;
        }

        @Override
        FileCompactStrategy getCompactStrategy() {
            return compactStrategy;
        }

        @Override
        FileCompactor getFileCompactor() {
            return fileCompactor;
        }

        @Override
        SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileWriterBucketStateSerializer(
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer());
        }

        @Override
        SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileSinkCommittableSerializer(
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
        }

        BucketWriter<IN, String> createBucketWriter() throws IOException {
            return new RowWiseBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
        }
    }

    /** Builder for the vanilla {@link FileSink} using a row format. */
    public static final class DefaultRowFormatBuilder<IN>
            extends RowFormatBuilder<IN, DefaultRowFormatBuilder<IN>> {
        private static final long serialVersionUID = -8503344257202146718L;

        private DefaultRowFormatBuilder(
                Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
            super(basePath, encoder, bucketAssigner);
        }
    }

    /** A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC. */
    @PublicEvolving
    public static class BulkFormatBuilder<IN, T extends BulkFormatBuilder<IN, T>>
            extends BucketsBuilder<IN, T> {

        private static final long serialVersionUID = 1L;

        private final Path basePath;

        private long bucketCheckInterval;

        private final BulkWriter.Factory<IN> writerFactory;

        private final FileWriterBucketFactory<IN> bucketFactory;

        private BucketAssigner<IN, String> bucketAssigner;

        private CheckpointRollingPolicy<IN, String> rollingPolicy;

        private OutputFileConfig outputFileConfig;

        private boolean isCompactDisabledExplicitly = false;

        private FileCompactStrategy compactStrategy;

        private FileCompactor fileCompactor;

        protected BulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            this(
                    basePath,
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    writerFactory,
                    assigner,
                    OnCheckpointRollingPolicy.build(),
                    new DefaultFileWriterBucketFactory<>(),
                    OutputFileConfig.builder().build());
        }

        protected BulkFormatBuilder(
                Path basePath,
                long bucketCheckInterval,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner,
                CheckpointRollingPolicy<IN, String> policy,
                FileWriterBucketFactory<IN> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = checkNotNull(basePath);
            this.bucketCheckInterval = bucketCheckInterval;
            this.writerFactory = writerFactory;
            this.bucketAssigner = checkNotNull(assigner);
            this.rollingPolicy = checkNotNull(policy);
            this.bucketFactory = checkNotNull(bucketFactory);
            this.outputFileConfig = checkNotNull(outputFileConfig);
        }

        public T withBucketCheckInterval(final long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(BucketAssigner<IN, String> assigner) {
            this.bucketAssigner = checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(CheckpointRollingPolicy<IN, String> rollingPolicy) {
            this.rollingPolicy = checkNotNull(rollingPolicy);
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

        public BulkFormatBuilder<IN, ? extends BulkFormatBuilder<IN, ?>> withNewBucketAssigner(
                BucketAssigner<IN, String> assigner) {
            checkState(
                    bucketFactory.getClass() == DefaultFileWriterBucketFactory.class,
                    "newBuilderWithBucketAssigner() cannot be called "
                            + "after specifying a customized bucket factory");
            return new BulkFormatBuilder<>(
                    basePath,
                    bucketCheckInterval,
                    writerFactory,
                    checkNotNull(assigner),
                    rollingPolicy,
                    bucketFactory,
                    outputFileConfig);
        }

        public T enableCompact(final FileCompactStrategy strategy, final FileCompactor compactor) {
            this.compactStrategy = checkNotNull(strategy);
            this.fileCompactor = checkNotNull(compactor);
            return self();
        }

        public T disableCompact() {
            this.isCompactDisabledExplicitly = true;
            return self();
        }

        /** Creates the actual sink. */
        public FileSink<IN> build() {
            return new FileSink<>(this);
        }

        @Override
        FileWriter<IN> createWriter(InitContext context) throws IOException {
            OutputFileConfig writerFileConfig;
            if (compactStrategy == null) {
                writerFileConfig = outputFileConfig;
            } else {
                // Compaction is enabled. We always commit before compacting, so the file written by
                // writer should be hid.
                writerFileConfig =
                        OutputFileConfig.builder()
                                .withPartPrefix("." + outputFileConfig.getPartPrefix())
                                .withPartSuffix(outputFileConfig.getPartSuffix())
                                .build();
            }

            return new FileWriter<>(
                    basePath,
                    context.metricGroup(),
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    writerFileConfig,
                    context.getProcessingTimeService(),
                    bucketCheckInterval);
        }

        @Override
        FileCommitter createCommitter() throws IOException {
            return new FileCommitter(createBucketWriter());
        }

        @Override
        boolean isCompactDisabledExplicitly() {
            return isCompactDisabledExplicitly;
        }

        @Override
        FileCompactStrategy getCompactStrategy() {
            return compactStrategy;
        }

        @Override
        FileCompactor getFileCompactor() {
            return fileCompactor;
        }

        @Override
        SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileWriterBucketStateSerializer(
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer());
        }

        @Override
        SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileSinkCommittableSerializer(
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
        }

        BucketWriter<IN, String> createBucketWriter() throws IOException {
            return new BulkBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), writerFactory);
        }
    }

    /**
     * Builder for the vanilla {@link FileSink} using a bulk format.
     *
     * @param <IN> record type
     */
    public static final class DefaultBulkFormatBuilder<IN>
            extends BulkFormatBuilder<IN, DefaultBulkFormatBuilder<IN>> {

        private static final long serialVersionUID = 7493169281036370228L;

        private DefaultBulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            super(basePath, writerFactory, assigner);
        }
    }
}
