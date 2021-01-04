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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveCompactReaderFactory;
import org.apache.flink.connectors.hive.write.HiveBulkWriterFactory;
import org.apache.flink.connectors.hive.write.HiveOutputFormatFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.writer.ThreadLocalClassLoaderConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogValidator;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.FileSystemOptions;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.filesystem.FileSystemTableSink;
import org.apache.flink.table.filesystem.FileSystemTableSink.TableBucketAssigner;
import org.apache.flink.table.filesystem.stream.PartitionCommitInfo;
import org.apache.flink.table.filesystem.stream.StreamingSink;
import org.apache.flink.table.filesystem.stream.compact.CompactReader;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.TypeDescription;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.checkAcidTable;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL;
import static org.apache.flink.table.filesystem.stream.compact.CompactOperator.convertToUncompacted;

/** Table sink to write to Hive tables. */
public class HiveTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableSink.class);

    private final ReadableConfig flinkConf;
    private final JobConf jobConf;
    private final CatalogTable catalogTable;
    private final ObjectIdentifier identifier;
    private final TableSchema tableSchema;
    private final String hiveVersion;
    private final HiveShim hiveShim;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();
    private boolean overwrite = false;
    private boolean dynamicGrouping = false;

    public HiveTableSink(
            ReadableConfig flinkConf,
            JobConf jobConf,
            ObjectIdentifier identifier,
            CatalogTable table) {
        this.flinkConf = flinkConf;
        this.jobConf = jobConf;
        this.identifier = identifier;
        this.catalogTable = table;
        hiveVersion =
                Preconditions.checkNotNull(
                        jobConf.get(HiveCatalogValidator.CATALOG_HIVE_VERSION),
                        "Hive version is not defined");
        hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        tableSchema = TableSchemaUtils.getPhysicalSchema(table.getSchema());
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter =
                context.createDataStructureConverter(tableSchema.toRowDataType());
        return (DataStreamSinkProvider)
                dataStream -> consume(dataStream, context.isBounded(), converter);
    }

    private DataStreamSink<?> consume(
            DataStream<RowData> dataStream, boolean isBounded, DataStructureConverter converter) {
        checkAcidTable(catalogTable, identifier.toObjectPath());

        try (HiveMetastoreClientWrapper client =
                HiveMetastoreClientFactory.create(
                        new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
            Table table = client.getTable(identifier.getDatabaseName(), identifier.getObjectName());
            StorageDescriptor sd = table.getSd();

            Class hiveOutputFormatClz =
                    hiveShim.getHiveOutputFormatClass(Class.forName(sd.getOutputFormat()));
            boolean isCompressed =
                    jobConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
            HiveWriterFactory writerFactory =
                    new HiveWriterFactory(
                            jobConf,
                            hiveOutputFormatClz,
                            sd.getSerdeInfo(),
                            tableSchema,
                            getPartitionKeyArray(),
                            HiveReflectionUtils.getTableMetadata(hiveShim, table),
                            hiveShim,
                            isCompressed);
            String extension =
                    Utilities.getFileExtension(
                            jobConf,
                            isCompressed,
                            (HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());

            OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder =
                    OutputFileConfig.builder()
                            .withPartPrefix("part-" + UUID.randomUUID().toString())
                            .withPartSuffix(extension == null ? "" : extension);

            if (isBounded) {
                OutputFileConfig fileNaming = fileNamingBuilder.build();
                return createBatchSink(dataStream, converter, sd, writerFactory, fileNaming);
            } else {
                if (overwrite) {
                    throw new IllegalStateException("Streaming mode not support overwrite.");
                }

                Properties tableProps = HiveReflectionUtils.getTableMetadata(hiveShim, table);
                return createStreamSink(
                        dataStream, sd, tableProps, writerFactory, fileNamingBuilder);
            }
        } catch (TException e) {
            throw new CatalogException("Failed to query Hive metaStore", e);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to create staging dir", e);
        } catch (ClassNotFoundException e) {
            throw new FlinkHiveException("Failed to get output format class", e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new FlinkHiveException("Failed to instantiate output format instance", e);
        }
    }

    private DataStreamSink<Row> createBatchSink(
            DataStream<RowData> dataStream,
            DataStructureConverter converter,
            StorageDescriptor sd,
            HiveWriterFactory recordWriterFactory,
            OutputFileConfig fileNaming)
            throws IOException {
        FileSystemOutputFormat.Builder<Row> builder = new FileSystemOutputFormat.Builder<>();
        builder.setPartitionComputer(
                new HiveRowPartitionComputer(
                        hiveShim,
                        defaultPartName(),
                        tableSchema.getFieldNames(),
                        tableSchema.getFieldDataTypes(),
                        getPartitionKeyArray()));
        builder.setDynamicGrouped(dynamicGrouping);
        builder.setPartitionColumns(getPartitionKeyArray());
        builder.setFileSystemFactory(fsFactory());
        builder.setFormatFactory(new HiveOutputFormatFactory(recordWriterFactory));
        builder.setMetaStoreFactory(msFactory());
        builder.setOverwrite(overwrite);
        builder.setStaticPartitions(staticPartitionSpec);
        builder.setTempPath(
                new org.apache.flink.core.fs.Path(toStagingDir(sd.getLocation(), jobConf)));
        builder.setOutputFileConfig(fileNaming);
        return dataStream
                .map((MapFunction<RowData, Row>) value -> (Row) converter.toExternal(value))
                .writeUsingOutputFormat(builder.build())
                .setParallelism(dataStream.getParallelism());
    }

    private DataStreamSink<?> createStreamSink(
            DataStream<RowData> dataStream,
            StorageDescriptor sd,
            Properties tableProps,
            HiveWriterFactory recordWriterFactory,
            OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder) {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        catalogTable.getOptions().forEach(conf::setString);

        HiveRowDataPartitionComputer partComputer =
                new HiveRowDataPartitionComputer(
                        hiveShim,
                        defaultPartName(),
                        tableSchema.getFieldNames(),
                        tableSchema.getFieldDataTypes(),
                        getPartitionKeyArray());
        TableBucketAssigner assigner = new TableBucketAssigner(partComputer);
        HiveRollingPolicy rollingPolicy =
                new HiveRollingPolicy(
                        conf.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                        conf.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis());

        boolean autoCompaction = conf.getBoolean(FileSystemOptions.AUTO_COMPACTION);

        if (autoCompaction) {
            fileNamingBuilder.withPartPrefix(
                    convertToUncompacted(fileNamingBuilder.build().getPartPrefix()));
        }
        OutputFileConfig outputFileConfig = fileNamingBuilder.build();

        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(sd.getLocation());

        BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> builder;
        if (flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER)) {
            builder =
                    bucketsBuilderForMRWriter(
                            recordWriterFactory, sd, assigner, rollingPolicy, outputFileConfig);
            LOG.info("Hive streaming sink: Use MapReduce RecordWriter writer.");
        } else {
            Optional<BulkWriter.Factory<RowData>> bulkFactory =
                    createBulkWriterFactory(getPartitionKeyArray(), sd);
            if (bulkFactory.isPresent()) {
                builder =
                        StreamingFileSink.forBulkFormat(
                                        path,
                                        new FileSystemTableSink.ProjectionBulkFactory(
                                                bulkFactory.get(), partComputer))
                                .withBucketAssigner(assigner)
                                .withRollingPolicy(rollingPolicy)
                                .withOutputFileConfig(outputFileConfig);
                LOG.info("Hive streaming sink: Use native parquet&orc writer.");
            } else {
                builder =
                        bucketsBuilderForMRWriter(
                                recordWriterFactory, sd, assigner, rollingPolicy, outputFileConfig);
                LOG.info(
                        "Hive streaming sink: Use MapReduce RecordWriter writer because BulkWriter Factory not available.");
            }
        }

        long bucketCheckInterval = conf.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis();

        DataStream<PartitionCommitInfo> writerStream;
        if (autoCompaction) {
            long compactionSize =
                    conf.getOptional(FileSystemOptions.COMPACTION_FILE_SIZE)
                            .orElse(conf.get(SINK_ROLLING_POLICY_FILE_SIZE))
                            .getBytes();

            writerStream =
                    StreamingSink.compactionWriter(
                            dataStream,
                            bucketCheckInterval,
                            builder,
                            fsFactory(),
                            path,
                            createCompactReaderFactory(sd, tableProps),
                            compactionSize);
        } else {
            writerStream = StreamingSink.writer(dataStream, bucketCheckInterval, builder);
        }

        return StreamingSink.sink(
                writerStream, path, identifier, getPartitionKeys(), msFactory(), fsFactory(), conf);
    }

    private String defaultPartName() {
        return jobConf.get(
                HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
                HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
    }

    private CompactReader.Factory<RowData> createCompactReaderFactory(
            StorageDescriptor sd, Properties properties) {
        return new HiveCompactReaderFactory(
                sd,
                properties,
                jobConf,
                catalogTable,
                hiveVersion,
                (RowType) tableSchema.toRowDataType().getLogicalType(),
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER));
    }

    private HiveTableMetaStoreFactory msFactory() {
        return new HiveTableMetaStoreFactory(
                jobConf, hiveVersion, identifier.getDatabaseName(), identifier.getObjectName());
    }

    private HadoopFileSystemFactory fsFactory() {
        return new HadoopFileSystemFactory(jobConf);
    }

    private BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>>
            bucketsBuilderForMRWriter(
                    HiveWriterFactory recordWriterFactory,
                    StorageDescriptor sd,
                    TableBucketAssigner assigner,
                    HiveRollingPolicy rollingPolicy,
                    OutputFileConfig outputFileConfig) {
        HiveBulkWriterFactory hadoopBulkFactory = new HiveBulkWriterFactory(recordWriterFactory);
        return new HadoopPathBasedBulkFormatBuilder<>(
                        new Path(sd.getLocation()), hadoopBulkFactory, jobConf, assigner)
                .withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(outputFileConfig);
    }

    private Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(
            String[] partitionColumns, StorageDescriptor sd) {
        String serLib = sd.getSerdeInfo().getSerializationLib().toLowerCase();
        int formatFieldCount = tableSchema.getFieldCount() - partitionColumns.length;
        String[] formatNames = new String[formatFieldCount];
        LogicalType[] formatTypes = new LogicalType[formatFieldCount];
        for (int i = 0; i < formatFieldCount; i++) {
            formatNames[i] = tableSchema.getFieldName(i).get();
            formatTypes[i] = tableSchema.getFieldDataType(i).get().getLogicalType();
        }
        RowType formatType = RowType.of(formatTypes, formatNames);
        if (serLib.contains("parquet")) {
            Configuration formatConf = new Configuration(jobConf);
            sd.getSerdeInfo().getParameters().forEach(formatConf::set);
            return Optional.of(
                    ParquetRowDataBuilder.createWriterFactory(
                            formatType, formatConf, hiveVersion.startsWith("3.")));
        } else if (serLib.contains("orc")) {
            Configuration formatConf = new ThreadLocalClassLoaderConfiguration(jobConf);
            sd.getSerdeInfo().getParameters().forEach(formatConf::set);
            TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(formatType);
            return Optional.of(
                    hiveShim.createOrcBulkWriterFactory(
                            formatConf, typeDescription.toString(), formatTypes));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return supportsGrouping;
    }

    // get a staging dir associated with a final dir
    private String toStagingDir(String finalDir, Configuration conf) throws IOException {
        String res = finalDir;
        if (!finalDir.endsWith(Path.SEPARATOR)) {
            res += Path.SEPARATOR;
        }
        // TODO: may append something more meaningful than a timestamp, like query ID
        res += ".staging_" + System.currentTimeMillis();
        Path path = new Path(res);
        FileSystem fs = path.getFileSystem(conf);
        Preconditions.checkState(
                fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
        fs.deleteOnExit(path);
        return res;
    }

    private List<String> getPartitionKeys() {
        return catalogTable.getPartitionKeys();
    }

    private String[] getPartitionKeyArray() {
        return getPartitionKeys().toArray(new String[0]);
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // make it a LinkedHashMap to maintain partition column order
        staticPartitionSpec = new LinkedHashMap<>();
        for (String partitionCol : getPartitionKeys()) {
            if (partition.containsKey(partitionCol)) {
                staticPartitionSpec.put(partitionCol, partition.get(partitionCol));
            }
        }
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSink copy() {
        HiveTableSink sink = new HiveTableSink(flinkConf, jobConf, identifier, catalogTable);
        sink.staticPartitionSpec = staticPartitionSpec;
        sink.overwrite = overwrite;
        sink.dynamicGrouping = dynamicGrouping;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "HiveSink";
    }

    /**
     * Getting size of the file is too expensive. See {@link HiveBulkWriterFactory#create}. We can't
     * check for every element, which will cause great pressure on DFS. Therefore, in this
     * implementation, only check the file size in {@link #shouldRollOnProcessingTime}, which can
     * effectively avoid DFS pressure.
     */
    private static class HiveRollingPolicy extends CheckpointRollingPolicy<RowData, String> {

        private final long rollingFileSize;
        private final long rollingTimeInterval;

        private HiveRollingPolicy(long rollingFileSize, long rollingTimeInterval) {
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
        }

        @Override
        public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
            return true;
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, RowData element) {
            return false;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) {
            try {
                return currentTime - partFileState.getCreationTime() >= rollingTimeInterval
                        || partFileState.getSize() > rollingFileSize;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
