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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.ContinuousPartitionFetcher;
import org.apache.flink.connector.file.table.DefaultPartTimeExtractor;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionContext;
import org.apache.flink.connectors.hive.read.HivePartitionFetcherContextBase;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetFormatStatisticsReportUtil;
import org.apache.flink.orc.util.OrcFormatStatisticsReportUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.FileBasedStatisticsReportableInputFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicFiltering;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_READ_STATISTICS_THREAD_NUM;
import static org.apache.flink.connectors.hive.util.HivePartitionUtils.getAllPartitions;

/** A TableSource implementation to read data from Hive tables. */
public class HiveTableSource
        implements ScanTableSource,
                SupportsPartitionPushDown,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsStatisticReport,
                SupportsDynamicFiltering {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableSource.class);
    private static final String HIVE_TRANSFORMATION = "hive";

    protected final JobConf jobConf;
    protected final ReadableConfig flinkConf;
    protected final ObjectPath tablePath;
    protected final ResolvedCatalogTable catalogTable;
    protected final String hiveVersion;
    protected final HiveShim hiveShim;

    // Remaining partition specs after partition pruning is performed. Null if pruning is not pushed
    // down.
    @Nullable protected List<Map<String, String>> remainingPartitions = null;
    @Nullable protected List<String> dynamicFilterPartitionKeys = null;
    protected int[] projectedFields;
    protected DataType producedDataType;
    protected Long limit = null;

    public HiveTableSource(
            JobConf jobConf,
            ReadableConfig flinkConf,
            ObjectPath tablePath,
            ResolvedCatalogTable catalogTable) {
        this.jobConf = Preconditions.checkNotNull(jobConf);
        this.flinkConf = Preconditions.checkNotNull(flinkConf);
        this.tablePath = Preconditions.checkNotNull(tablePath);
        this.catalogTable = Preconditions.checkNotNull(catalogTable);
        this.hiveVersion =
                Preconditions.checkNotNull(
                        jobConf.get(HiveCatalogFactoryOptions.HIVE_VERSION.key()),
                        "Hive version is not defined");
        this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        this.producedDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return getDataStream(providerContext, execEnv);
            }

            @Override
            public boolean isBounded() {
                return !isStreamingSource();
            }
        };
    }

    @VisibleForTesting
    protected DataStream<RowData> getDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        HiveSourceBuilder sourceBuilder =
                new HiveSourceBuilder(jobConf, flinkConf, tablePath, hiveVersion, catalogTable)
                        .setProjectedFields(projectedFields)
                        .setLimit(limit);

        if (isStreamingSource()) {
            DataStreamSource<RowData> sourceStream =
                    toDataStreamSource(execEnv, sourceBuilder.buildWithDefaultBulkFormat());
            providerContext.generateUid(HIVE_TRANSFORMATION).ifPresent(sourceStream::uid);
            return sourceStream;
        } else {
            List<HiveTablePartition> hivePartitionsToRead =
                    getAllPartitions(
                            jobConf,
                            hiveVersion,
                            tablePath,
                            catalogTable.getPartitionKeys(),
                            remainingPartitions);

            int parallelism =
                    new HiveParallelismInference(tablePath, flinkConf)
                            .infer(
                                    () ->
                                            HiveSourceFileEnumerator.getNumFiles(
                                                    hivePartitionsToRead, jobConf),
                                    () ->
                                            HiveSourceFileEnumerator.createInputSplits(
                                                            0, hivePartitionsToRead, jobConf, true)
                                                    .size())
                            .limit(limit);
            return toDataStreamSource(
                            execEnv,
                            sourceBuilder
                                    .setPartitions(hivePartitionsToRead)
                                    .setDynamicFilterPartitionKeys(dynamicFilterPartitionKeys)
                                    .buildWithDefaultBulkFormat())
                    .setParallelism(parallelism);
        }
    }

    private DataStreamSource<RowData> toDataStreamSource(
            StreamExecutionEnvironment execEnv, HiveSource<RowData> hiveSource) {
        return execEnv.fromSource(
                hiveSource,
                WatermarkStrategy.noWatermarks(),
                "HiveSource-" + tablePath.getFullName());
    }

    protected boolean isStreamingSource() {
        return Boolean.parseBoolean(
                catalogTable
                        .getOptions()
                        .getOrDefault(
                                STREAMING_SOURCE_ENABLE.key(),
                                STREAMING_SOURCE_ENABLE.defaultValue().toString()));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        return Optional.empty();
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        if (catalogTable.getPartitionKeys() != null
                && catalogTable.getPartitionKeys().size() != 0) {
            this.remainingPartitions = remainingPartitions;
        } else {
            throw new UnsupportedOperationException(
                    "Should not apply partitions to a non-partitioned table.");
        }
    }

    @Override
    public List<String> listAcceptedFilterFields() {
        List<String> acceptedFilterFields = new ArrayList<>();
        for (String partitionKey : catalogTable.getPartitionKeys()) {
            // Only partition keys with supported types can be returned as accepted filter fields.
            if (HiveSourceDynamicFileEnumerator.SUPPORTED_TYPES.contains(
                    catalogTable
                            .getResolvedSchema()
                            .getColumn(partitionKey)
                            .map(Column::getDataType)
                            .map(DataType::getLogicalType)
                            .map(LogicalType::getTypeRoot)
                            .orElse(null))) {
                acceptedFilterFields.add(partitionKey);
            }
        }

        return acceptedFilterFields;
    }

    @Override
    public void applyDynamicFiltering(List<String> candidateFilterFields) {
        if (catalogTable.isPartitioned()) {
            dynamicFilterPartitionKeys = candidateFilterFields;
        } else {
            throw new TableException(
                    String.format(
                            "Hive source table : %s is not a partition table, but try to apply dynamic filtering.",
                            catalogTable));
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        this.producedDataType = producedDataType;
    }

    @Override
    public String asSummaryString() {
        return "HiveSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        HiveTableSource source = new HiveTableSource(jobConf, flinkConf, tablePath, catalogTable);
        source.remainingPartitions = remainingPartitions;
        source.projectedFields = projectedFields;
        source.producedDataType = producedDataType;
        source.limit = limit;
        source.dynamicFilterPartitionKeys = dynamicFilterPartitionKeys;
        return source;
    }

    @Override
    public TableStats reportStatistics() {
        try {
            // only support BOUNDED source
            if (isStreamingSource()) {
                return TableStats.UNKNOWN;
            }
            if (flinkConf.get(FileSystemConnectorOptions.SOURCE_REPORT_STATISTICS)
                    != FileSystemConnectorOptions.FileStatisticsType.ALL) {
                return TableStats.UNKNOWN;
            }

            HiveSourceBuilder sourceBuilder =
                    new HiveSourceBuilder(jobConf, flinkConf, tablePath, hiveVersion, catalogTable)
                            .setProjectedFields(projectedFields);
            List<HiveTablePartition> hivePartitionsToRead =
                    getAllPartitions(
                            jobConf,
                            hiveVersion,
                            tablePath,
                            catalogTable.getPartitionKeys(),
                            remainingPartitions);
            BulkFormat<RowData, HiveSourceSplit> defaultBulkFormat =
                    sourceBuilder.createDefaultBulkFormat();
            List<HiveSourceSplit> inputSplits =
                    HiveSourceFileEnumerator.createInputSplits(
                            1, hivePartitionsToRead, jobConf, false);
            if (inputSplits.size() != 0) {
                TableStats tableStats;
                if (defaultBulkFormat instanceof FileBasedStatisticsReportableInputFormat) {
                    // If HiveInputFormat's variable useMapRedReader is false, Flink will use hadoop
                    // mapRed record to read data.
                    tableStats =
                            ((FileBasedStatisticsReportableInputFormat) defaultBulkFormat)
                                    .reportStatistics(
                                            inputSplits.stream()
                                                    .map(FileSourceSplit::path)
                                                    .collect(Collectors.toList()),
                                            catalogTable
                                                    .getResolvedSchema()
                                                    .toPhysicalRowDataType());
                } else {
                    // If HiveInputFormat's variable useMapRedReader is true, Hive using MapRed
                    // InputFormat to read data.
                    tableStats =
                            getMapRedInputFormatStatistics(
                                    inputSplits,
                                    catalogTable.getResolvedSchema().toPhysicalRowDataType());
                }
                if (limit == null) {
                    // If no limit push down, return recompute table stats.
                    return tableStats;
                } else {
                    // If table have limit push down, return new table stats without table column
                    // stats.
                    long newRowCount = Math.min(limit, tableStats.getRowCount());
                    return new TableStats(newRowCount);
                }
            } else {
                return new TableStats(0);
            }

        } catch (Exception e) {
            LOG.warn("Reporting statistics failed for hive table source: {}", e.getMessage());
            return TableStats.UNKNOWN;
        }
    }

    private TableStats getMapRedInputFormatStatistics(
            List<HiveSourceSplit> inputSplits, DataType producedDataType) {
        // TODO now we assume that one hive external table has only one storage file format
        String serializationLib =
                inputSplits
                        .get(0)
                        .getHiveTablePartition()
                        .getStorageDescriptor()
                        .getSerdeInfo()
                        .getSerializationLib()
                        .toLowerCase();
        List<Path> files =
                inputSplits.stream().map(FileSourceSplit::path).collect(Collectors.toList());
        int statisticsThreadNum = flinkConf.get(TABLE_EXEC_HIVE_READ_STATISTICS_THREAD_NUM);
        Preconditions.checkArgument(
                statisticsThreadNum >= 1,
                TABLE_EXEC_HIVE_READ_STATISTICS_THREAD_NUM.key() + " cannot be less than 1");
        // Now we only support Parquet, Orc formats.
        if (serializationLib.contains("parquet")) {
            return ParquetFormatStatisticsReportUtil.getTableStatistics(
                    files,
                    producedDataType,
                    jobConf,
                    hiveVersion.startsWith("3"),
                    statisticsThreadNum);
        } else if (serializationLib.contains("orc")) {
            return OrcFormatStatisticsReportUtil.getTableStatistics(
                    files, producedDataType, jobConf, statisticsThreadNum);
        } else {
            // Now, only support Orc and Parquet Formats.
            LOG.info(
                    "Now for hive table source, reporting statistics only support Orc and Parquet formats.");
            return TableStats.UNKNOWN;
        }
    }

    /** PartitionFetcher.Context for {@link ContinuousPartitionFetcher}. */
    @SuppressWarnings("unchecked")
    public static class HiveContinuousPartitionFetcherContext<T extends Comparable<T>>
            extends HivePartitionFetcherContextBase<Partition>
            implements HiveContinuousPartitionContext<Partition, T> {

        private static final long serialVersionUID = 1L;
        private static final Long DEFAULT_MIN_TIME_OFFSET = 0L;
        private static final String DEFAULT_MIN_NAME_OFFSET = "";

        private final TypeSerializer<T> typeSerializer;
        private final T consumeStartOffset;

        public HiveContinuousPartitionFetcherContext(
                ObjectPath tablePath,
                HiveShim hiveShim,
                JobConfWrapper confWrapper,
                List<String> partitionKeys,
                Configuration configuration,
                String defaultPartitionName) {
            super(
                    tablePath,
                    hiveShim,
                    confWrapper,
                    partitionKeys,
                    configuration,
                    defaultPartitionName);

            switch (partitionOrder) {
                case PARTITION_NAME:
                    if (configuration.contains(STREAMING_SOURCE_CONSUME_START_OFFSET)) {
                        String consumeOffsetStr =
                                configuration.getString(STREAMING_SOURCE_CONSUME_START_OFFSET);
                        consumeStartOffset = (T) consumeOffsetStr;
                    } else {
                        consumeStartOffset = (T) DEFAULT_MIN_NAME_OFFSET;
                    }
                    typeSerializer = (TypeSerializer<T>) StringSerializer.INSTANCE;
                    break;
                case PARTITION_TIME:
                case CREATE_TIME:
                    if (configuration.contains(STREAMING_SOURCE_CONSUME_START_OFFSET)) {
                        String consumeOffsetStr =
                                configuration.getString(STREAMING_SOURCE_CONSUME_START_OFFSET);

                        LocalDateTime localDateTime =
                                DefaultPartTimeExtractor.toLocalDateTime(
                                        consumeOffsetStr,
                                        configuration.getString(
                                                PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER));

                        consumeStartOffset =
                                (T)
                                        Long.valueOf(
                                                TimestampData.fromLocalDateTime(localDateTime)
                                                        .getMillisecond());
                    } else {
                        consumeStartOffset = (T) DEFAULT_MIN_TIME_OFFSET;
                    }
                    typeSerializer = (TypeSerializer<T>) LongSerializer.INSTANCE;
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported partition order: " + partitionOrder);
            }
        }

        @Override
        public Optional<Partition> getPartition(List<String> partValues) throws TException {
            try {
                return Optional.of(
                        metaStoreClient.getPartition(
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                partValues));
            } catch (NoSuchObjectException e) {
                return Optional.empty();
            }
        }

        @Override
        public ObjectPath getTablePath() {
            return tablePath;
        }

        /** Convert partition to HiveTablePartition. */
        public HiveTablePartition toHiveTablePartition(Partition partition) {
            return HivePartitionUtils.toHiveTablePartition(partitionKeys, tableProps, partition);
        }

        @Override
        public TypeSerializer<T> getTypeSerializer() {
            return typeSerializer;
        }

        @Override
        public T getConsumeStartOffset() {
            return consumeStartOffset;
        }

        @Override
        public void close() throws Exception {
            if (this.metaStoreClient != null) {
                this.metaStoreClient.close();
            }
        }
    }

    @VisibleForTesting
    public JobConf getJobConf() {
        return jobConf;
    }
}
