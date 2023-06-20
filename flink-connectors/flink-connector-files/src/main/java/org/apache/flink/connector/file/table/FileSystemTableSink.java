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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
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
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.StreamingSink;
import org.apache.flink.connector.file.table.stream.compact.CompactBulkReader;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
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
import java.util.stream.Stream;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.AUTO_COMPACTION;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.COMPACTION_FILE_SIZE;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_INACTIVITY_INTERVAL;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL;
import static org.apache.flink.connector.file.table.stream.compact.CompactOperator.convertToUncompacted;

/** File system {@link DynamicTableSink}. */
@Internal
public class FileSystemTableSink extends AbstractFileSystemTable
        implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    // For compaction reading
    @Nullable private final DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat;
    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> deserializationFormat;

    // For Writing
    @Nullable private final EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
    @Nullable private final EncodingFormat<SerializationSchema<RowData>> serializationFormat;

    private boolean overwrite = false;
    private boolean dynamicGrouping = false;
    private LinkedHashMap<String, String> staticPartitions = new LinkedHashMap<>();

    @Nullable private Integer configuredParallelism;

    FileSystemTableSink(
            ObjectIdentifier tableIdentifier,
            DataType physicalRowDataType,
            List<String> partitionKeys,
            ReadableConfig tableOptions,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat,
            @Nullable EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
            @Nullable EncodingFormat<SerializationSchema<RowData>> serializationFormat) {
        super(tableIdentifier, physicalRowDataType, partitionKeys, tableOptions);
        this.bulkReaderFormat = bulkReaderFormat;
        this.deserializationFormat = deserializationFormat;
        if (Stream.of(bulkWriterFormat, serializationFormat).allMatch(Objects::isNull)) {
            String identifier = tableOptions.get(FactoryUtil.FORMAT);
            throw new ValidationException(
                    String.format(
                            "Could not find any format factory for identifier '%s' in the classpath.",
                            identifier));
        }
        this.bulkWriterFormat = bulkWriterFormat;
        this.serializationFormat = serializationFormat;
        this.configuredParallelism =
                this.tableOptions.get(FileSystemConnectorOptions.SINK_PARALLELISM);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                return consume(providerContext, dataStream, sinkContext);
            }
        };
    }

    private DataStreamSink<?> consume(
            ProviderContext providerContext, DataStream<RowData> dataStream, Context sinkContext) {
        final int inputParallelism = dataStream.getParallelism();
        final int parallelism = Optional.ofNullable(configuredParallelism).orElse(inputParallelism);
        boolean parallelismConfigued = configuredParallelism != null;

        if (sinkContext.isBounded()) {
            return createBatchSink(dataStream, sinkContext, parallelism, parallelismConfigued);
        } else {
            if (overwrite) {
                throw new IllegalStateException("Streaming mode not support overwrite.");
            }

            return createStreamingSink(
                    providerContext, dataStream, sinkContext, parallelism, parallelismConfigued);
        }
    }

    private RowDataPartitionComputer partitionComputer() {
        return new RowDataPartitionComputer(
                defaultPartName,
                DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                partitionKeys.toArray(new String[0]));
    }

    private DataStreamSink<RowData> createBatchSink(
            DataStream<RowData> inputStream,
            Context sinkContext,
            final int parallelism,
            boolean parallelismConfigured) {
        FileSystemOutputFormat.Builder<RowData> builder = new FileSystemOutputFormat.Builder<>();
        builder.setPartitionComputer(partitionComputer())
                .setDynamicGrouped(dynamicGrouping)
                .setPartitionColumns(partitionKeys.toArray(new String[0]))
                .setFormatFactory(createOutputFormatFactory(sinkContext))
                .setMetaStoreFactory(new EmptyMetaStoreFactory(path))
                .setOverwrite(overwrite)
                .setStaticPartitions(staticPartitions)
                .setTempPath(toStagingPath())
                .setOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("part-" + UUID.randomUUID())
                                .build())
                .setPartitionCommitPolicyFactory(
                        new PartitionCommitPolicyFactory(
                                tableOptions.get(
                                        FileSystemConnectorOptions
                                                .SINK_PARTITION_COMMIT_POLICY_KIND),
                                tableOptions.get(
                                        FileSystemConnectorOptions
                                                .SINK_PARTITION_COMMIT_POLICY_CLASS),
                                tableOptions.get(
                                        FileSystemConnectorOptions
                                                .SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME),
                                tableOptions.get(
                                        FileSystemConnectorOptions
                                                .SINK_PARTITION_COMMIT_POLICY_CLASS_PARAMETERS)));

        DataStreamSink<RowData> sink = inputStream.writeUsingOutputFormat(builder.build());
        sink.getTransformation().setParallelism(parallelism, parallelismConfigured);
        return sink.name("Filesystem");
    }

    private DataStreamSink<?> createStreamingSink(
            ProviderContext providerContext,
            DataStream<RowData> dataStream,
            Context sinkContext,
            final int parallelism,
            boolean parallelismConfigured) {
        FileSystemFactory fsFactory = FileSystem::get;
        RowDataPartitionComputer computer = partitionComputer();

        boolean autoCompaction = tableOptions.getBoolean(AUTO_COMPACTION);
        Object writer = createWriter(sinkContext);
        boolean isEncoder = writer instanceof Encoder;
        TableBucketAssigner assigner = new TableBucketAssigner(computer);
        TableRollingPolicy rollingPolicy =
                new TableRollingPolicy(
                        !isEncoder || autoCompaction,
                        tableOptions.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                        tableOptions.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis(),
                        tableOptions.get(SINK_ROLLING_POLICY_INACTIVITY_INTERVAL).toMillis());

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
                            .getOptional(COMPACTION_FILE_SIZE)
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
                            providerContext,
                            dataStream,
                            bucketCheckInterval,
                            bucketsBuilder,
                            fsFactory,
                            path,
                            reader,
                            compactionSize,
                            parallelism,
                            parallelismConfigured);
        } else {
            writerStream =
                    StreamingSink.writer(
                            providerContext,
                            dataStream,
                            bucketCheckInterval,
                            bucketsBuilder,
                            parallelism,
                            partitionKeys,
                            tableOptions,
                            parallelismConfigured);
        }

        return StreamingSink.sink(
                providerContext,
                writerStream,
                path,
                tableIdentifier,
                partitionKeys,
                new EmptyMetaStoreFactory(path),
                fsFactory,
                tableOptions);
    }

    private Optional<CompactReader.Factory<RowData>> createCompactReaderFactory(Context context) {
        // Compute producedDataType (including partition fields) and physicalDataType (excluding
        // partition fields)
        final DataType producedDataType = physicalRowDataType;
        final DataType physicalDataType =
                DataType.getFields(producedDataType).stream()
                        .filter(field -> !partitionKeys.contains(field.getName()))
                        .collect(Collectors.collectingAndThen(Collectors.toList(), DataTypes::ROW));

        if (bulkReaderFormat != null) {
            final BulkFormat<RowData, FileSourceSplit> format =
                    new FileInfoExtractorBulkFormat(
                            bulkReaderFormat.createRuntimeDecoder(
                                    createSourceContext(context), physicalDataType),
                            producedDataType,
                            context.createTypeInformation(producedDataType),
                            Collections.emptyMap(),
                            partitionKeys,
                            defaultPartName);
            return Optional.of(CompactBulkReader.factory(format));
        } else if (deserializationFormat != null) {
            final DeserializationSchema<RowData> decoder =
                    deserializationFormat.createRuntimeDecoder(
                            createSourceContext(context), physicalDataType);
            final BulkFormat<RowData, FileSourceSplit> format =
                    new FileInfoExtractorBulkFormat(
                            new DeserializationSchemaAdapter(decoder),
                            producedDataType,
                            context.createTypeInformation(producedDataType),
                            Collections.emptyMap(),
                            partitionKeys,
                            defaultPartName);
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
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                return context.createTypeInformation(producedLogicalType);
            }

            @Override
            public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                    DataType producedDataType) {
                // This method cannot be implemented without changing the
                // DynamicTableSink.DataStructureConverter interface
                throw new UnsupportedOperationException(
                        "Compaction reader not support DataStructure converter.");
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
        DataType physicalDataTypeWithoutPartitionColumns =
                DataType.getFields(physicalRowDataType).stream()
                        .filter(field -> !partitionKeys.contains(field.getName()))
                        .collect(Collectors.collectingAndThen(Collectors.toList(), DataTypes::ROW));
        if (bulkWriterFormat != null) {
            return bulkWriterFormat.createRuntimeEncoder(
                    sinkContext, physicalDataTypeWithoutPartitionColumns);
        } else if (serializationFormat != null) {
            return new SerializationSchemaAdapter(
                    serializationFormat.createRuntimeEncoder(
                            sinkContext, physicalDataTypeWithoutPartitionColumns));
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
                            FileSystemConnectorOptions.SINK_PARALLELISM.key(),
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
            private transient FSDataOutputStream stream;

            @Override
            public void configure(Configuration parameters) {}

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                this.stream = path.getFileSystem().create(path, FileSystem.WriteMode.OVERWRITE);
                this.writer = factory.create(stream);
            }

            @Override
            public void writeRecord(RowData record) throws IOException {
                writer.addElement(record);
            }

            @Override
            public void close() throws IOException {
                writer.flush();
                writer.finish();
                stream.close();
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
                        tableIdentifier,
                        physicalRowDataType,
                        partitionKeys,
                        tableOptions,
                        bulkReaderFormat,
                        deserializationFormat,
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
        private final long inactivityInterval;

        public TableRollingPolicy(
                boolean rollOnCheckpoint,
                long rollingFileSize,
                long rollingTimeInterval,
                long inactivityInterval) {
            this.rollOnCheckpoint = rollOnCheckpoint;
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            Preconditions.checkArgument(inactivityInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
            this.inactivityInterval = inactivityInterval;
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
            return currentTime - partFileState.getCreationTime() >= rollingTimeInterval
                    || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
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
