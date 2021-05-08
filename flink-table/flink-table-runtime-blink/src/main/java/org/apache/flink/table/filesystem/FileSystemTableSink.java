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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.StreamingSink;
import org.apache.flink.table.filesystem.stream.compact.CompactBulkReader;
import org.apache.flink.table.filesystem.stream.compact.CompactReader;
import org.apache.flink.table.filesystem.stream.compact.FileInputFormatCompactReader;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL;
import static org.apache.flink.table.filesystem.stream.compact.CompactOperator.convertToUncompacted;

/** File system {@link DynamicTableSink}. */
public class FileSystemTableSink extends AbstractFileSystemTable
        implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    // For compaction reading
    @Nullable private final DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat;
    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> deserializationFormat;
    @Nullable private final FileSystemFormatFactory formatFactory;

    // For Writing
    @Nullable private final EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
    @Nullable private final EncodingFormat<SerializationSchema<RowData>> serializationFormat;

    private boolean overwrite = false;
    private boolean dynamicGrouping = false;
    private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

    @Nullable private Integer configuredParallelism;

    FileSystemTableSink(
            DynamicTableFactory.Context context,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat,
            @Nullable FileSystemFormatFactory formatFactory,
            @Nullable EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
            @Nullable EncodingFormat<SerializationSchema<RowData>> serializationFormat) {
        super(context);
        this.bulkReaderFormat = bulkReaderFormat;
        this.deserializationFormat = deserializationFormat;
        this.formatFactory = formatFactory;
        if (Stream.of(bulkWriterFormat, serializationFormat, formatFactory)
                .allMatch(Objects::isNull)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            throw new ValidationException(
                    String.format(
                            "Could not find any format factory for identifier '%s' in the classpath.",
                            identifier));
        }
        this.bulkWriterFormat = bulkWriterFormat;
        this.serializationFormat = serializationFormat;
        this.configuredParallelism = tableOptions.get(FileSystemOptions.SINK_PARALLELISM);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        return (DataStreamSinkProvider) dataStream -> consume(dataStream, sinkContext);
    }

    private DataStreamSink<?> consume(DataStream<RowData> dataStream, Context sinkContext) {
        final int inputParallelism = dataStream.getParallelism();
        final int parallelism = Optional.ofNullable(configuredParallelism).orElse(inputParallelism);

        if (sinkContext.isBounded()) {
            return createBatchSink(dataStream, sinkContext, parallelism);
        } else {
            if (overwrite) {
                throw new IllegalStateException("Streaming mode not support overwrite.");
            }

            return createStreamingSink(dataStream, sinkContext, parallelism);
        }
    }

    private RowDataPartitionComputer partitionComputer() {
        return new RowDataPartitionComputer(
                defaultPartName,
                schema.getFieldNames(),
                schema.getFieldDataTypes(),
                partitionKeys.toArray(new String[0]));
    }

    private DataStreamSink<RowData> createBatchSink(
            DataStream<RowData> inputStream, Context sinkContext, final int parallelism) {
        FileSystemOutputFormat.Builder<RowData> builder = new FileSystemOutputFormat.Builder<>();
        builder.setPartitionComputer(partitionComputer());
        builder.setDynamicGrouped(dynamicGrouping);
        builder.setPartitionColumns(partitionKeys.toArray(new String[0]));
        builder.setFormatFactory(createOutputFormatFactory(sinkContext));
        builder.setMetaStoreFactory(new EmptyMetaStoreFactory(path));
        builder.setOverwrite(overwrite);
        builder.setStaticPartitions(staticPartitions);
        builder.setTempPath(toStagingPath());
        builder.setOutputFileConfig(
                OutputFileConfig.builder()
                        .withPartPrefix("part-" + UUID.randomUUID().toString())
                        .build());
        return inputStream
                .writeUsingOutputFormat(builder.build())
                .setParallelism(parallelism)
                .name("Filesystem");
    }

    private DataStreamSink<?> createStreamingSink(
            DataStream<RowData> dataStream, Context sinkContext, final int parallelism) {
        FileSystemFactory fsFactory = FileSystem::get;
        RowDataPartitionComputer computer = partitionComputer();

        boolean autoCompaction = tableOptions.getBoolean(FileSystemOptions.AUTO_COMPACTION);
        Object writer = createWriter(sinkContext);
        boolean isEncoder = writer instanceof Encoder;
        TableBucketAssigner assigner = new TableBucketAssigner(computer);
        TableRollingPolicy rollingPolicy =
                new TableRollingPolicy(
                        !isEncoder || autoCompaction,
                        tableOptions.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                        tableOptions.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis());

        String randomPrefix = "part-" + UUID.randomUUID().toString();
        OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder = OutputFileConfig.builder();
        fileNamingBuilder =
                autoCompaction
                        ? fileNamingBuilder.withPartPrefix(convertToUncompacted(randomPrefix))
                        : fileNamingBuilder.withPartPrefix(randomPrefix);
        OutputFileConfig fileNamingConfig = fileNamingBuilder.build();

        BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> bucketsBuilder;
        if (isEncoder) {
            //noinspection unchecked
            bucketsBuilder =
                    StreamingFileSink.forRowFormat(
                                    path,
                                    new ProjectionEncoder((Encoder<RowData>) writer, computer))
                            .withBucketAssigner(assigner)
                            .withOutputFileConfig(fileNamingConfig)
                            .withRollingPolicy(rollingPolicy);
        } else {
            //noinspection unchecked
            bucketsBuilder =
                    StreamingFileSink.forBulkFormat(
                                    path,
                                    new ProjectionBulkFactory(
                                            (BulkWriter.Factory<RowData>) writer, computer))
                            .withBucketAssigner(assigner)
                            .withOutputFileConfig(fileNamingConfig)
                            .withRollingPolicy(rollingPolicy);
        }

        long bucketCheckInterval = tableOptions.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis();

        DataStream<PartitionCommitInfo> writerStream;
        if (autoCompaction) {
            long compactionSize =
                    tableOptions
                            .getOptional(FileSystemOptions.COMPACTION_FILE_SIZE)
                            .orElse(tableOptions.get(SINK_ROLLING_POLICY_FILE_SIZE))
                            .getBytes();

            CompactReader.Factory<RowData> reader =
                    createCompactReaderFactory(sinkContext)
                            .orElseThrow(
                                    () ->
                                            new TableException(
                                                    "Please implement available reader for compaction:"
                                                            + " BulkFormat, FileInputFormat."));

            writerStream =
                    StreamingSink.compactionWriter(
                            dataStream,
                            bucketCheckInterval,
                            bucketsBuilder,
                            fsFactory,
                            path,
                            reader,
                            compactionSize,
                            parallelism);
        } else {
            writerStream =
                    StreamingSink.writer(
                            dataStream, bucketCheckInterval, bucketsBuilder, parallelism);
        }

        return StreamingSink.sink(
                writerStream,
                path,
                tableIdentifier,
                partitionKeys,
                new EmptyMetaStoreFactory(path),
                fsFactory,
                tableOptions);
    }

    private Optional<CompactReader.Factory<RowData>> createCompactReaderFactory(Context context) {
        DataType producedDataType = schema.toRowDataType();
        if (bulkReaderFormat != null) {
            BulkFormat<RowData, FileSourceSplit> format =
                    bulkReaderFormat.createRuntimeDecoder(
                            createSourceContext(context), producedDataType);
            return Optional.of(CompactBulkReader.factory(format));
        } else if (formatFactory != null) {
            InputFormat<RowData, ?> format = formatFactory.createReader(createReaderContext());
            if (format instanceof FileInputFormat) {
                //noinspection unchecked
                return Optional.of(
                        FileInputFormatCompactReader.factory((FileInputFormat<RowData>) format));
            }
        } else if (deserializationFormat != null) {
            // NOTE, we need pass full format types to deserializationFormat
            DeserializationSchema<RowData> decoder =
                    deserializationFormat.createRuntimeDecoder(
                            createSourceContext(context), getFormatDataType());
            int[] projectedFields = IntStream.range(0, schema.getFieldCount()).toArray();
            DeserializationSchemaAdapter format =
                    new DeserializationSchemaAdapter(
                            decoder, schema, projectedFields, partitionKeys, defaultPartName);
            return Optional.of(CompactBulkReader.factory(format));
        }
        return Optional.empty();
    }

    private DynamicTableSource.Context createSourceContext(Context context) {
        return new DynamicTableSource.Context() {
            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                return context.createTypeInformation(producedDataType);
            }

            @Override
            public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                    DataType producedDataType) {
                throw new TableException("Compaction reader not support DataStructure converter.");
            }
        };
    }

    private FileSystemFormatFactory.ReaderContext createReaderContext() {
        return new FileSystemFormatFactory.ReaderContext() {
            @Override
            public TableSchema getSchema() {
                return schema;
            }

            @Override
            public ReadableConfig getFormatOptions() {
                return formatOptions(formatFactory.factoryIdentifier());
            }

            @Override
            public List<String> getPartitionKeys() {
                return partitionKeys;
            }

            @Override
            public String getDefaultPartName() {
                return defaultPartName;
            }

            @Override
            public Path[] getPaths() {
                return new Path[] {path};
            }

            @Override
            public int[] getProjectFields() {
                return IntStream.range(0, schema.getFieldCount()).toArray();
            }

            @Override
            public long getPushedDownLimit() {
                return Long.MAX_VALUE;
            }

            @Override
            public List<ResolvedExpression> getPushedDownFilters() {
                return Collections.emptyList();
            }
        };
    }

    private Path toStagingPath() {
        Path stagingDir = new Path(path, ".staging_" + System.currentTimeMillis());
        try {
            FileSystem fs = stagingDir.getFileSystem();
            Preconditions.checkState(
                    fs.exists(stagingDir) || fs.mkdirs(stagingDir),
                    "Failed to create staging dir " + stagingDir);
            return stagingDir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private OutputFormatFactory<RowData> createOutputFormatFactory(Context sinkContext) {
        Object writer = createWriter(sinkContext);
        return writer instanceof Encoder
                ? path -> createEncoderOutputFormat((Encoder<RowData>) writer, path)
                : path -> createBulkWriterOutputFormat((BulkWriter.Factory<RowData>) writer, path);
    }

    private Object createWriter(Context sinkContext) {
        if (bulkWriterFormat != null) {
            return bulkWriterFormat.createRuntimeEncoder(sinkContext, getFormatDataType());
        } else if (serializationFormat != null) {
            return new SerializationSchemaAdapter(
                    serializationFormat.createRuntimeEncoder(sinkContext, getFormatDataType()));
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    private void checkConfiguredParallelismAllowed(ChangelogMode requestChangelogMode) {
        final Integer parallelism = this.configuredParallelism;
        if (parallelism == null) {
            return;
        }
        if (!requestChangelogMode.containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    String.format(
                            "Currently, filesystem sink doesn't support setting parallelism (%d) by '%s' "
                                    + "when the input stream is not INSERT only. The row kinds of input stream are [%s]",
                            parallelism,
                            FileSystemOptions.SINK_PARALLELISM.key(),
                            requestChangelogMode.getContainedKinds().stream()
                                    .map(RowKind::shortString)
                                    .collect(Collectors.joining(","))));
        }
    }

    private static OutputFormat<RowData> createBulkWriterOutputFormat(
            BulkWriter.Factory<RowData> factory, Path path) {
        return new OutputFormat<RowData>() {

            private static final long serialVersionUID = 1L;

            private transient BulkWriter<RowData> writer;

            @Override
            public void configure(Configuration parameters) {}

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                this.writer =
                        factory.create(
                                path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE));
            }

            @Override
            public void writeRecord(RowData record) throws IOException {
                writer.addElement(record);
            }

            @Override
            public void close() throws IOException {
                writer.flush();
                writer.finish();
            }
        };
    }

    private static OutputFormat<RowData> createEncoderOutputFormat(
            Encoder<RowData> encoder, Path path) {
        return new OutputFormat<RowData>() {

            private static final long serialVersionUID = 1L;

            private transient FSDataOutputStream output;

            @Override
            public void configure(Configuration parameters) {}

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                this.output = path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE);
            }

            @Override
            public void writeRecord(RowData record) throws IOException {
                encoder.encode(record, output);
            }

            @Override
            public void close() throws IOException {
                this.output.flush();
                this.output.close();
            }
        };
    }

    private LinkedHashMap<String, String> toPartialLinkedPartSpec(Map<String, String> part) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        for (String partitionKey : partitionKeys) {
            if (part.containsKey(partitionKey)) {
                partSpec.put(partitionKey, part.get(partitionKey));
            }
        }
        return partSpec;
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return dynamicGrouping;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        checkConfiguredParallelismAllowed(requestedMode);
        if (bulkWriterFormat != null) {
            return bulkWriterFormat.getChangelogMode();
        } else if (serializationFormat != null) {
            return serializationFormat.getChangelogMode();
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    @Override
    public DynamicTableSink copy() {
        FileSystemTableSink sink =
                new FileSystemTableSink(
                        context,
                        bulkReaderFormat,
                        deserializationFormat,
                        formatFactory,
                        bulkWriterFormat,
                        serializationFormat);
        sink.overwrite = overwrite;
        sink.dynamicGrouping = dynamicGrouping;
        sink.staticPartitions = staticPartitions;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "Filesystem";
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        this.staticPartitions = toPartialLinkedPartSpec(partition);
    }

    /** Table bucket assigner, wrap {@link PartitionComputer}. */
    public static class TableBucketAssigner implements BucketAssigner<RowData, String> {

        private final PartitionComputer<RowData> computer;

        public TableBucketAssigner(PartitionComputer<RowData> computer) {
            this.computer = computer;
        }

        @Override
        public String getBucketId(RowData element, Context context) {
            try {
                return PartitionPathUtils.generatePartitionPath(
                        computer.generatePartValues(element));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    /** Table {@link RollingPolicy}, it extends {@link CheckpointRollingPolicy} for bulk writers. */
    public static class TableRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

        private final boolean rollOnCheckpoint;
        private final long rollingFileSize;
        private final long rollingTimeInterval;

        public TableRollingPolicy(
                boolean rollOnCheckpoint, long rollingFileSize, long rollingTimeInterval) {
            this.rollOnCheckpoint = rollOnCheckpoint;
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
        }

        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
            try {
                return rollOnCheckpoint || partFileState.getSize() > rollingFileSize;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element)
                throws IOException {
            return partFileState.getSize() > rollingFileSize;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) {
            return currentTime - partFileState.getCreationTime() >= rollingTimeInterval;
        }
    }

    private static class ProjectionEncoder implements Encoder<RowData> {

        private final Encoder<RowData> encoder;
        private final RowDataPartitionComputer computer;

        private ProjectionEncoder(Encoder<RowData> encoder, RowDataPartitionComputer computer) {
            this.encoder = encoder;
            this.computer = computer;
        }

        @Override
        public void encode(RowData element, OutputStream stream) throws IOException {
            encoder.encode(computer.projectColumnsToWrite(element), stream);
        }
    }

    /** Project row to non-partition fields. */
    public static class ProjectionBulkFactory implements BulkWriter.Factory<RowData> {

        private final BulkWriter.Factory<RowData> factory;
        private final RowDataPartitionComputer computer;

        public ProjectionBulkFactory(
                BulkWriter.Factory<RowData> factory, RowDataPartitionComputer computer) {
            this.factory = factory;
            this.computer = computer;
        }

        @Override
        public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
            BulkWriter<RowData> writer = factory.create(out);
            return new BulkWriter<RowData>() {

                @Override
                public void addElement(RowData element) throws IOException {
                    writer.addElement(computer.projectColumnsToWrite(element));
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }
            };
        }
    }
}
