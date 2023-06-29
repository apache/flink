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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.EmptyMetaStoreFactory;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.FileSystemOutputFormat;
import org.apache.flink.connector.file.table.FileSystemTableSink;
import org.apache.flink.connector.file.table.FileSystemTableSink.TableBucketAssigner;
import org.apache.flink.connector.file.table.PartitionCommitPolicy;
import org.apache.flink.connector.file.table.PartitionCommitPolicyFactory;
import org.apache.flink.connector.file.table.TableMetaStoreFactory;
import org.apache.flink.connector.file.table.batch.BatchSink;
import org.apache.flink.connector.file.table.batch.compact.BatchCompactOperator;
import org.apache.flink.connector.file.table.batch.compact.BatchFileWriter;
import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.connector.file.table.stream.StreamingSink;
import org.apache.flink.connector.file.table.stream.compact.CompactMessages.CoordinatorInput;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.connectors.hive.read.HiveCompactReaderFactory;
import org.apache.flink.connectors.hive.util.HiveConfUtils;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.connectors.hive.write.HiveBulkWriterFactory;
import org.apache.flink.connectors.hive.write.HiveOutputFormatFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.writer.ThreadLocalClassLoaderConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.HadoopPathBasedBulkFormatBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BucketsBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_FILE_SIZE;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_INACTIVITY_INTERVAL;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL;
import static org.apache.flink.connector.file.table.stream.compact.CompactOperator.convertToUncompacted;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.checkAcidTable;
import static org.apache.flink.table.planner.delegation.hive.HiveParserConstants.IS_INSERT_DIRECTORY;
import static org.apache.flink.table.planner.delegation.hive.HiveParserConstants.IS_TO_LOCAL_DIRECTORY;

/** Table sink to write to Hive tables. */
public class HiveTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableSink.class);

    public static final String BATCH_COMPACT_WRITER_OP_NAME = "batch_writer";

    private final boolean fallbackMappedReader;
    private final boolean fallbackMappedWriter;
    private final JobConf jobConf;
    private final ResolvedCatalogTable catalogTable;
    private final ObjectIdentifier identifier;
    private final ResolvedSchema resolvedSchema;
    private final String hiveVersion;
    private final HiveShim hiveShim;
    private final boolean dynamicGroupingEnabled;

    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();
    private boolean overwrite = false;
    private boolean dynamicGrouping = false;
    private final boolean autoGatherStatistic;
    private final int gatherStatsThreadNum;

    @Nullable private final Integer configuredSinkParallelism;

    public HiveTableSink(
            ReadableConfig flinkConf,
            JobConf jobConf,
            ObjectIdentifier identifier,
            ResolvedCatalogTable table,
            @Nullable Integer configuredSinkParallelism) {
        this(
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER),
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_WRITER),
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_DYNAMIC_GROUPING_ENABLED),
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE),
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_THREAD_NUM),
                jobConf,
                identifier,
                table,
                configuredSinkParallelism);
    }

    private HiveTableSink(
            boolean fallbackMappedReader,
            boolean fallbackMappedWriter,
            boolean dynamicGroupingEnabled,
            boolean autoGatherStatistic,
            int gatherStatsThreadNum,
            JobConf jobConf,
            ObjectIdentifier identifier,
            ResolvedCatalogTable table,
            @Nullable Integer configuredSinkParallelism) {
        this.fallbackMappedReader = fallbackMappedReader;
        this.fallbackMappedWriter = fallbackMappedWriter;
        this.dynamicGroupingEnabled = dynamicGroupingEnabled;
        this.autoGatherStatistic = autoGatherStatistic;
        this.gatherStatsThreadNum = gatherStatsThreadNum;
        this.jobConf = jobConf;
        this.identifier = identifier;
        this.catalogTable = table;
        hiveVersion =
                Preconditions.checkNotNull(
                        jobConf.get(HiveCatalogFactoryOptions.HIVE_VERSION.key()),
                        "Hive version is not defined");
        hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        resolvedSchema = table.getResolvedSchema();
        this.configuredSinkParallelism = configuredSinkParallelism;
        validateAutoGatherStatistic(autoGatherStatistic, catalogTable);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter =
                context.createDataStructureConverter(resolvedSchema.toPhysicalRowDataType());
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                return consume(providerContext, dataStream, context.isBounded(), converter);
            }
        };
    }

    private void validateAutoGatherStatistic(
            boolean autoGatherStatistic, CatalogTable catalogTable) {
        if (autoGatherStatistic && catalogTable.isPartitioned()) {
            // if auto gather statistic and the table is partitioned,
            // the table's option "SINK_PARTITION_COMMIT_POLICY_KIND" should contain 'metastore'
            org.apache.flink.configuration.Configuration flinkConf =
                    org.apache.flink.configuration.Configuration.fromMap(catalogTable.getOptions());
            String policyKind = flinkConf.getString(HiveOptions.SINK_PARTITION_COMMIT_POLICY_KIND);
            String[] policyStrings = policyKind.split(",");
            Arrays.stream(policyStrings)
                    .filter(policy -> policy.equalsIgnoreCase(PartitionCommitPolicy.METASTORE))
                    .findAny()
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "When %s is set to true, the option %s for the table %s should contains %s.",
                                                    HiveOptions
                                                            .TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE
                                                            .key(),
                                                    FileSystemConnectorOptions
                                                            .SINK_PARTITION_COMMIT_POLICY_CLASS,
                                                    identifier,
                                                    PartitionCommitPolicy.METASTORE)));
        }
    }

    private DataStreamSink<?> consume(
            ProviderContext providerContext,
            DataStream<RowData> dataStream,
            boolean isBounded,
            DataStructureConverter converter) {
        checkAcidTable(catalogTable.getOptions(), identifier.toObjectPath());

        StorageDescriptor sd;
        Properties tableProps = new Properties();
        Map<String, String> options = catalogTable.getOptions();
        boolean isInsertDirectory =
                Boolean.parseBoolean(
                        options.getOrDefault(
                                CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX + IS_INSERT_DIRECTORY,
                                "false"));
        boolean isToLocal = false;
        if (isInsertDirectory) {
            isToLocal =
                    Boolean.parseBoolean(
                            options.getOrDefault(
                                    CatalogPropertiesUtil.FLINK_PROPERTY_PREFIX
                                            + IS_TO_LOCAL_DIRECTORY,
                                    "false"));
            sd =
                    org.apache.hadoop.hive.ql.metadata.Table.getEmptyTable(
                                    identifier.getDatabaseName(), identifier.getObjectName())
                            .getSd();
            HiveConf hiveConf = HiveConfUtils.create(jobConf);
            HiveTableUtil.setDefaultStorageFormatForDirectory(sd, HiveConfUtils.create(jobConf));
            HiveTableUtil.extractRowFormat(sd, catalogTable.getOptions());
            HiveTableUtil.extractStoredAs(sd, catalogTable.getOptions(), hiveConf);
            HiveTableUtil.extractLocation(sd, catalogTable.getOptions());
            tableProps.putAll(sd.getSerdeInfo().getParameters());
            tableProps.putAll(catalogTable.getOptions());
        } else {
            try (HiveMetastoreClientWrapper client =
                    HiveMetastoreClientFactory.create(HiveConfUtils.create(jobConf), hiveVersion)) {
                Table table =
                        client.getTable(identifier.getDatabaseName(), identifier.getObjectName());
                sd = table.getSd();
                tableProps = HiveReflectionUtils.getTableMetadata(hiveShim, table);
            } catch (TException e) {
                throw new CatalogException("Failed to query Hive metaStore", e);
            }
        }

        try {
            Class hiveOutputFormatClz =
                    hiveShim.getHiveOutputFormatClass(Class.forName(sd.getOutputFormat()));
            boolean isCompressed =
                    jobConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
            HiveWriterFactory writerFactory =
                    new HiveWriterFactory(
                            jobConf,
                            hiveOutputFormatClz,
                            sd.getSerdeInfo(),
                            resolvedSchema,
                            getPartitionKeyArray(),
                            tableProps,
                            hiveShim,
                            isCompressed);
            String extension =
                    Utilities.getFileExtension(
                            jobConf,
                            isCompressed,
                            (HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());

            OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder =
                    OutputFileConfig.builder()
                            .withPartPrefix("part-" + UUID.randomUUID())
                            .withPartSuffix(extension == null ? "" : extension);

            final int sinkParallelism =
                    Optional.ofNullable(configuredSinkParallelism)
                            .orElse(dataStream.getParallelism());
            boolean sinkParallelismConfigured = configuredSinkParallelism != null;
            if (isBounded) {
                TableMetaStoreFactory msFactory =
                        isInsertDirectory
                                ? new EmptyMetaStoreFactory(
                                        new org.apache.flink.core.fs.Path(sd.getLocation()))
                                : msFactory(autoGatherStatistic);
                // default to use the dest location as the parent directory of staging directory
                String stagingParentDir = sd.getLocation();
                if (isToLocal) {
                    // it's for writing to local file system, dest location is a path in local file
                    // system. we need to know the scratch path for
                    // non-local file system, so that it'll first write to the scratch path and then
                    // move to the local path
                    Path rootScratchDirPath =
                            new Path(
                                    HiveConf.getVar(
                                            HiveConfUtils.create(jobConf),
                                            HiveConf.ConfVars.SCRATCHDIR));
                    // TODO: may append something more meaningful than a timestamp, like query ID
                    Path scratchDir =
                            new Path(
                                    rootScratchDirPath, String.valueOf(System.currentTimeMillis()));
                    stagingParentDir = scratchDir.toUri().toString();
                }
                return createBatchSink(
                        dataStream,
                        converter,
                        writerFactory,
                        msFactory,
                        fileNamingBuilder,
                        stagingParentDir,
                        sd,
                        tableProps,
                        isToLocal,
                        overwrite,
                        sinkParallelism,
                        sinkParallelismConfigured);
            } else {
                if (overwrite) {
                    throw new IllegalStateException("Streaming mode not support overwrite.");
                }
                return createStreamSink(
                        providerContext,
                        dataStream,
                        sd,
                        tableProps,
                        writerFactory,
                        fileNamingBuilder,
                        sinkParallelism,
                        sinkParallelismConfigured);
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to create staging dir", e);
        } catch (ClassNotFoundException e) {
            throw new FlinkHiveException("Failed to get output format class", e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new FlinkHiveException("Failed to instantiate output format instance", e);
        }
    }

    private DataStreamSink<?> createBatchSink(
            DataStream<RowData> dataStream,
            DataStructureConverter converter,
            HiveWriterFactory recordWriterFactory,
            TableMetaStoreFactory metaStoreFactory,
            OutputFileConfig.OutputFileConfigBuilder fileConfigBuilder,
            String stagingParentDir,
            StorageDescriptor sd,
            Properties tableProps,
            boolean isToLocal,
            boolean overwrite,
            int sinkParallelism,
            boolean sinkParallelismConfigured)
            throws IOException {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        catalogTable.getOptions().forEach(conf::setString);
        boolean autoCompaction = conf.getBoolean(FileSystemConnectorOptions.AUTO_COMPACTION);
        if (autoCompaction) {
            Optional<Integer> compactParallelismOptional =
                    conf.getOptional(FileSystemConnectorOptions.COMPACTION_PARALLELISM);
            int compactParallelism = compactParallelismOptional.orElse(sinkParallelism);
            boolean compactParallelismConfigured =
                    compactParallelismOptional.isPresent()
                            ||
                            // if only sink parallelism is set, compact operator should follow this
                            // setting. that means its parallelism equals to sink and marked as
                            // configured to disable auto parallelism inference.
                            sinkParallelismConfigured;
            return createBatchCompactSink(
                    dataStream,
                    converter,
                    recordWriterFactory,
                    metaStoreFactory,
                    fileConfigBuilder
                            .withPartPrefix(
                                    BatchCompactOperator.UNCOMPACTED_PREFIX
                                            + "part-"
                                            + UUID.randomUUID())
                            .build(),
                    stagingParentDir,
                    sd,
                    tableProps,
                    isToLocal,
                    overwrite,
                    sinkParallelism,
                    compactParallelism,
                    sinkParallelismConfigured,
                    compactParallelismConfigured);
        } else {
            return createBatchNoCompactSink(
                    dataStream,
                    converter,
                    recordWriterFactory,
                    metaStoreFactory,
                    fileConfigBuilder.build(),
                    stagingParentDir,
                    isToLocal,
                    sinkParallelism,
                    sinkParallelismConfigured);
        }
    }

    private DataStreamSink<?> createBatchCompactSink(
            DataStream<RowData> dataStream,
            DataStructureConverter converter,
            HiveWriterFactory recordWriterFactory,
            TableMetaStoreFactory metaStoreFactory,
            OutputFileConfig fileNaming,
            String stagingParentDir,
            StorageDescriptor sd,
            Properties tableProps,
            boolean isToLocal,
            boolean overwrite,
            final int sinkParallelism,
            final int compactParallelism,
            boolean sinkParallelismConfigured,
            boolean compactParallelismConfigured)
            throws IOException {
        String[] partitionColumns = getPartitionKeyArray();
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        catalogTable.getOptions().forEach(conf::setString);
        HadoopFileSystemFactory fsFactory = fsFactory();
        org.apache.flink.core.fs.Path tmpPath =
                new org.apache.flink.core.fs.Path(toStagingDir(stagingParentDir, jobConf));

        PartitionCommitPolicyFactory partitionCommitPolicyFactory =
                new PartitionCommitPolicyFactory(
                        conf.get(HiveOptions.SINK_PARTITION_COMMIT_POLICY_KIND),
                        conf.get(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS),
                        conf.get(
                                FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME),
                        conf.get(
                                FileSystemConnectorOptions
                                        .SINK_PARTITION_COMMIT_POLICY_CLASS_PARAMETERS));

        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(sd.getLocation());
        BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> builder =
                getBucketsBuilder(path, recordWriterFactory, sd, fileNaming, conf);

        CompactReader.Factory<RowData> readerFactory = createCompactReaderFactory(sd, tableProps);

        HiveOutputFormatFactory outputFormatFactory =
                new HiveOutputFormatFactory(recordWriterFactory);
        HiveRowPartitionComputer partitionComputer =
                new HiveRowPartitionComputer(
                        hiveShim,
                        JobConfUtils.getDefaultPartitionName(jobConf),
                        resolvedSchema.getColumnNames().toArray(new String[0]),
                        resolvedSchema.getColumnDataTypes().toArray(new DataType[0]),
                        partitionColumns);

        SingleOutputStreamOperator<Row> map =
                dataStream.map(value -> (Row) converter.toExternal(value));
        map.getTransformation().setParallelism(sinkParallelism, sinkParallelismConfigured);

        DataStream<CoordinatorInput> writerDataStream =
                map.transform(
                        BATCH_COMPACT_WRITER_OP_NAME,
                        TypeInformation.of(CoordinatorInput.class),
                        new BatchFileWriter<>(
                                fsFactory,
                                tmpPath,
                                partitionColumns,
                                dynamicGrouping,
                                staticPartitionSpec,
                                outputFormatFactory,
                                partitionComputer,
                                fileNaming));
        writerDataStream
                .getTransformation()
                .setParallelism(sinkParallelism, sinkParallelismConfigured);

        long compactAverageSize = conf.get(HiveOptions.COMPACT_SMALL_FILES_AVG_SIZE).getBytes();

        long compactTargetSize =
                conf.getOptional(FileSystemConnectorOptions.COMPACTION_FILE_SIZE)
                        .orElse(conf.get(SINK_ROLLING_POLICY_FILE_SIZE))
                        .getBytes();

        return BatchSink.createBatchCompactSink(
                writerDataStream,
                builder,
                readerFactory,
                fsFactory,
                metaStoreFactory,
                partitionCommitPolicyFactory,
                partitionColumns,
                staticPartitionSpec,
                tmpPath,
                identifier,
                compactAverageSize,
                compactTargetSize,
                isToLocal,
                overwrite,
                compactParallelism,
                compactParallelismConfigured);
    }

    private BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>>
            getBucketsBuilder(
                    org.apache.flink.core.fs.Path path,
                    HiveWriterFactory recordWriterFactory,
                    StorageDescriptor sd,
                    OutputFileConfig fileNaming,
                    org.apache.flink.configuration.Configuration conf) {
        HiveRowDataPartitionComputer partComputer =
                new HiveRowDataPartitionComputer(
                        hiveShim,
                        JobConfUtils.getDefaultPartitionName(jobConf),
                        resolvedSchema.getColumnNames().toArray(new String[0]),
                        resolvedSchema.getColumnDataTypes().toArray(new DataType[0]),
                        getPartitionKeyArray());
        TableBucketAssigner assigner = new TableBucketAssigner(partComputer);
        HiveRollingPolicy rollingPolicy =
                new HiveRollingPolicy(
                        conf.get(SINK_ROLLING_POLICY_FILE_SIZE).getBytes(),
                        conf.get(SINK_ROLLING_POLICY_ROLLOVER_INTERVAL).toMillis(),
                        conf.get(SINK_ROLLING_POLICY_INACTIVITY_INTERVAL).toMillis());

        BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> builder;
        if (fallbackMappedWriter) {
            builder =
                    bucketsBuilderForMRWriter(
                            recordWriterFactory, sd, assigner, rollingPolicy, fileNaming);
            LOG.info("Hive sink: Use MapReduce RecordWriter writer.");
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
                                .withOutputFileConfig(fileNaming);
                LOG.info("Hive sink: Use native parquet&orc writer.");
            } else {
                builder =
                        bucketsBuilderForMRWriter(
                                recordWriterFactory, sd, assigner, rollingPolicy, fileNaming);
                LOG.info(
                        "Hive sink: Use MapReduce RecordWriter writer because BulkWriter Factory not available.");
            }
        }
        return builder;
    }

    private DataStreamSink<Row> createBatchNoCompactSink(
            DataStream<RowData> dataStream,
            DataStructureConverter converter,
            HiveWriterFactory recordWriterFactory,
            TableMetaStoreFactory metaStoreFactory,
            OutputFileConfig fileNaming,
            String stagingParentDir,
            boolean isToLocal,
            final int sinkParallelism,
            boolean sinkParallelismConfigured)
            throws IOException {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        catalogTable.getOptions().forEach(conf::setString);
        FileSystemOutputFormat.Builder<Row> builder = new FileSystemOutputFormat.Builder<>();
        builder.setPartitionComputer(
                new HiveRowPartitionComputer(
                        hiveShim,
                        JobConfUtils.getDefaultPartitionName(jobConf),
                        resolvedSchema.getColumnNames().toArray(new String[0]),
                        resolvedSchema.getColumnDataTypes().toArray(new DataType[0]),
                        getPartitionKeyArray()));
        builder.setDynamicGrouped(dynamicGrouping);
        builder.setPartitionColumns(getPartitionKeyArray());
        builder.setFileSystemFactory(fsFactory());
        builder.setFormatFactory(new HiveOutputFormatFactory(recordWriterFactory));
        builder.setMetaStoreFactory(metaStoreFactory);
        builder.setOverwrite(overwrite);
        builder.setIsToLocal(isToLocal);
        builder.setStaticPartitions(staticPartitionSpec);
        builder.setTempPath(
                new org.apache.flink.core.fs.Path(toStagingDir(stagingParentDir, jobConf)));
        builder.setOutputFileConfig(fileNaming);
        builder.setIdentifier(identifier);
        builder.setPartitionCommitPolicyFactory(
                new PartitionCommitPolicyFactory(
                        conf.get(HiveOptions.SINK_PARTITION_COMMIT_POLICY_KIND),
                        conf.get(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_CLASS),
                        conf.get(
                                FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME),
                        conf.get(
                                FileSystemConnectorOptions
                                        .SINK_PARTITION_COMMIT_POLICY_CLASS_PARAMETERS)));
        return BatchSink.createBatchNoCompactSink(
                dataStream, converter, builder.build(), sinkParallelism, sinkParallelismConfigured);
    }

    private DataStreamSink<?> createStreamSink(
            ProviderContext providerContext,
            DataStream<RowData> dataStream,
            StorageDescriptor sd,
            Properties tableProps,
            HiveWriterFactory recordWriterFactory,
            OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder,
            final int parallelism,
            boolean parallelismConfigured) {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        catalogTable.getOptions().forEach(conf::setString);

        String commitPolicies =
                conf.getString(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND);
        if (!getPartitionKeys().isEmpty() && StringUtils.isNullOrWhitespaceOnly(commitPolicies)) {
            throw new FlinkHiveException(
                    String.format(
                            "Streaming write to partitioned hive table %s without providing a commit policy. "
                                    + "Make sure to set a proper value for %s",
                            identifier,
                            FileSystemConnectorOptions.SINK_PARTITION_COMMIT_POLICY_KIND.key()));
        }

        boolean autoCompaction = conf.getBoolean(FileSystemConnectorOptions.AUTO_COMPACTION);
        if (autoCompaction) {
            fileNamingBuilder.withPartPrefix(
                    convertToUncompacted(fileNamingBuilder.build().getPartPrefix()));
        }
        OutputFileConfig outputFileConfig = fileNamingBuilder.build();

        org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(sd.getLocation());
        BucketsBuilder<RowData, String, ? extends BucketsBuilder<RowData, ?, ?>> builder =
                getBucketsBuilder(path, recordWriterFactory, sd, outputFileConfig, conf);

        long bucketCheckInterval = conf.get(SINK_ROLLING_POLICY_CHECK_INTERVAL).toMillis();

        DataStream<PartitionCommitInfo> writerStream;
        if (autoCompaction) {
            long compactionSize =
                    conf.getOptional(FileSystemConnectorOptions.COMPACTION_FILE_SIZE)
                            .orElse(conf.get(SINK_ROLLING_POLICY_FILE_SIZE))
                            .getBytes();

            writerStream =
                    StreamingSink.compactionWriter(
                            providerContext,
                            dataStream,
                            bucketCheckInterval,
                            builder,
                            fsFactory(),
                            path,
                            createCompactReaderFactory(sd, tableProps),
                            compactionSize,
                            parallelism,
                            parallelismConfigured);
        } else {
            writerStream =
                    StreamingSink.writer(
                            providerContext,
                            dataStream,
                            bucketCheckInterval,
                            builder,
                            parallelism,
                            getPartitionKeys(),
                            conf,
                            parallelismConfigured);
        }

        return StreamingSink.sink(
                providerContext,
                writerStream,
                path,
                identifier,
                getPartitionKeys(),
                // todo: support gather statistic in FLINK-29027
                msFactory(false),
                fsFactory(),
                conf);
    }

    private CompactReader.Factory<RowData> createCompactReaderFactory(
            StorageDescriptor sd, Properties properties) {
        return new HiveCompactReaderFactory(
                sd,
                properties,
                jobConf,
                catalogTable,
                hiveVersion,
                (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType(),
                fallbackMappedReader);
    }

    private HiveTableMetaStoreFactory msFactory(boolean autoGatherStatistic) {
        return new HiveTableMetaStoreFactory(
                jobConf,
                fsFactory(),
                hiveVersion,
                identifier.getDatabaseName(),
                identifier.getObjectName(),
                org.apache.flink.configuration.Configuration.fromMap(catalogTable.getOptions())
                        .get(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME),
                autoGatherStatistic,
                gatherStatsThreadNum);
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
        int formatFieldCount = resolvedSchema.getColumns().size() - partitionColumns.length;
        String[] formatNames = new String[formatFieldCount];
        LogicalType[] formatTypes = new LogicalType[formatFieldCount];
        for (int i = 0; i < formatFieldCount; i++) {
            formatNames[i] = resolvedSchema.getColumnNames().get(i);
            formatTypes[i] = resolvedSchema.getColumnDataTypes().get(i).getLogicalType();
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
        if (supportsGrouping && dynamicGroupingEnabled) {
            this.dynamicGrouping = true;
            return true;
        } else {
            return false;
        }
    }

    // get a staging dir
    private String toStagingDir(String stagingParentDir, Configuration conf) throws IOException {
        if (!stagingParentDir.endsWith(Path.SEPARATOR)) {
            stagingParentDir += Path.SEPARATOR;
        }
        // TODO: may append something more meaningful than a timestamp, like query ID
        stagingParentDir += ".staging_" + System.currentTimeMillis();
        Path path = new Path(stagingParentDir);
        FileSystem fs = path.getFileSystem(conf);
        Preconditions.checkState(
                fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
        fs.deleteOnExit(path);
        return stagingParentDir;
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
        HiveTableSink sink =
                new HiveTableSink(
                        fallbackMappedReader,
                        fallbackMappedWriter,
                        dynamicGroupingEnabled,
                        autoGatherStatistic,
                        gatherStatsThreadNum,
                        jobConf,
                        identifier,
                        catalogTable,
                        configuredSinkParallelism);
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
        private final long inactivityInterval;

        private HiveRollingPolicy(
                long rollingFileSize, long rollingTimeInterval, long inactivityInterval) {
            Preconditions.checkArgument(rollingFileSize > 0L);
            Preconditions.checkArgument(rollingTimeInterval > 0L);
            Preconditions.checkArgument(inactivityInterval > 0L);
            this.rollingFileSize = rollingFileSize;
            this.rollingTimeInterval = rollingTimeInterval;
            this.inactivityInterval = inactivityInterval;
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
                        || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval
                        || partFileState.getSize() > rollingFileSize;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @VisibleForTesting
    public JobConf getJobConf() {
        return jobConf;
    }
}
