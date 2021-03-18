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
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionContext;
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionFetcher;
import org.apache.flink.connectors.hive.read.HivePartitionFetcherContextBase;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connectors.hive.util.HivePartitionUtils.getAllPartitions;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.checkAcidTable;
import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_ORDER;

/** A TableSource implementation to read data from Hive tables. */
public class HiveTableSource
        implements ScanTableSource,
                SupportsPartitionPushDown,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(HiveTableSource.class);
    private static final Duration DEFAULT_SCAN_MONITOR_INTERVAL = Duration.ofMinutes(1L);

    protected final JobConf jobConf;
    protected final ReadableConfig flinkConf;
    protected final ObjectPath tablePath;
    protected final CatalogTable catalogTable;
    protected final String hiveVersion;
    protected final HiveShim hiveShim;

    // Remaining partition specs after partition pruning is performed. Null if pruning is not pushed
    // down.
    @Nullable private List<Map<String, String>> remainingPartitions = null;
    protected int[] projectedFields;
    private Long limit = null;

    public HiveTableSource(
            JobConf jobConf,
            ReadableConfig flinkConf,
            ObjectPath tablePath,
            CatalogTable catalogTable) {
        this.jobConf = Preconditions.checkNotNull(jobConf);
        this.flinkConf = Preconditions.checkNotNull(flinkConf);
        this.tablePath = Preconditions.checkNotNull(tablePath);
        this.catalogTable = Preconditions.checkNotNull(catalogTable);
        this.hiveVersion =
                Preconditions.checkNotNull(
                        jobConf.get(HiveCatalogFactoryOptions.HIVE_VERSION.key()),
                        "Hive version is not defined");
        this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                return getDataStream(execEnv);
            }

            @Override
            public boolean isBounded() {
                return !isStreamingSource();
            }
        };
    }

    @VisibleForTesting
    protected DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
        validateScanConfigurations();
        checkAcidTable(catalogTable, tablePath);
        List<HiveTablePartition> allHivePartitions =
                getAllPartitions(
                        jobConf,
                        hiveVersion,
                        tablePath,
                        catalogTable,
                        hiveShim,
                        remainingPartitions);
        Configuration configuration = Configuration.fromMap(catalogTable.getOptions());

        HiveSource.HiveSourceBuilder sourceBuilder =
                new HiveSource.HiveSourceBuilder(
                        jobConf,
                        tablePath,
                        catalogTable,
                        allHivePartitions,
                        limit,
                        hiveVersion,
                        flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER),
                        (RowType) getProducedDataType().getLogicalType());
        if (isStreamingSource()) {
            if (catalogTable.getPartitionKeys().isEmpty()) {
                String consumeOrderStr = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
                ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
                if (consumeOrder != ConsumeOrder.CREATE_TIME_ORDER) {
                    throw new UnsupportedOperationException(
                            "Only "
                                    + ConsumeOrder.CREATE_TIME_ORDER
                                    + " is supported for non partition table.");
                }
            }

            Duration monitorInterval =
                    configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL) == null
                            ? DEFAULT_SCAN_MONITOR_INTERVAL
                            : configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);
            sourceBuilder.monitorContinuously(monitorInterval);

            if (!catalogTable.getPartitionKeys().isEmpty()) {
                sourceBuilder.setFetcher(new HiveContinuousPartitionFetcher());
                final String defaultPartitionName =
                        jobConf.get(
                                HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
                                HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
                HiveContinuousPartitionFetcherContext<?> fetcherContext =
                        new HiveContinuousPartitionFetcherContext(
                                tablePath,
                                hiveShim,
                                new JobConfWrapper(jobConf),
                                catalogTable.getPartitionKeys(),
                                getTableSchema().getFieldDataTypes(),
                                getTableSchema().getFieldNames(),
                                configuration,
                                defaultPartitionName);
                sourceBuilder.setFetcherContext(fetcherContext);
            }
        }

        HiveSource hiveSource = sourceBuilder.build();
        DataStreamSource<RowData> source =
                execEnv.fromSource(
                        hiveSource,
                        WatermarkStrategy.noWatermarks(),
                        "HiveSource-" + tablePath.getFullName());

        if (isStreamingSource()) {
            return source;
        } else {
            int parallelism =
                    new HiveParallelismInference(tablePath, flinkConf)
                            .infer(
                                    () ->
                                            HiveSourceFileEnumerator.getNumFiles(
                                                    allHivePartitions, jobConf),
                                    () ->
                                            HiveSourceFileEnumerator.createInputSplits(
                                                            0, allHivePartitions, jobConf)
                                                    .size())
                            .limit(limit);
            return source.setParallelism(parallelism);
        }
    }

    private void validateScanConfigurations() {
        String partitionInclude =
                catalogTable
                        .getOptions()
                        .getOrDefault(
                                STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                                STREAMING_SOURCE_PARTITION_INCLUDE.defaultValue());
        Preconditions.checkArgument(
                "all".equals(partitionInclude),
                String.format(
                        "The only supported '%s' is 'all' in hive table scan, but is '%s'",
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(), partitionInclude));
    }

    protected boolean isStreamingSource() {
        return Boolean.parseBoolean(
                catalogTable
                        .getOptions()
                        .getOrDefault(
                                STREAMING_SOURCE_ENABLE.key(),
                                STREAMING_SOURCE_ENABLE.defaultValue().toString()));
    }

    protected TableSchema getTableSchema() {
        return catalogTable.getSchema();
    }

    private DataType getProducedDataType() {
        return getProducedTableSchema().toRowDataType().bridgedTo(RowData.class);
    }

    protected TableSchema getProducedTableSchema() {
        TableSchema fullSchema = getTableSchema();
        if (projectedFields == null) {
            return fullSchema;
        } else {
            String[] fullNames = fullSchema.getFieldNames();
            DataType[] fullTypes = fullSchema.getFieldDataTypes();
            return TableSchema.builder()
                    .fields(
                            Arrays.stream(projectedFields)
                                    .mapToObj(i -> fullNames[i])
                                    .toArray(String[]::new),
                            Arrays.stream(projectedFields)
                                    .mapToObj(i -> fullTypes[i])
                                    .toArray(DataType[]::new))
                    .build();
        }
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
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
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
        source.limit = limit;
        return source;
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
                DataType[] fieldTypes,
                String[] fieldNames,
                Configuration configuration,
                String defaultPartitionName) {
            super(
                    tablePath,
                    hiveShim,
                    confWrapper,
                    partitionKeys,
                    fieldTypes,
                    fieldNames,
                    configuration,
                    defaultPartitionName);

            switch (consumeOrder) {
                case PARTITION_NAME_ORDER:
                    if (configuration.contains(STREAMING_SOURCE_CONSUME_START_OFFSET)) {
                        String consumeOffsetStr =
                                configuration.getString(STREAMING_SOURCE_CONSUME_START_OFFSET);
                        consumeStartOffset = (T) consumeOffsetStr;
                    } else {
                        consumeStartOffset = (T) DEFAULT_MIN_NAME_OFFSET;
                    }
                    typeSerializer = (TypeSerializer<T>) StringSerializer.INSTANCE;
                    break;
                case PARTITION_TIME_ORDER:
                case CREATE_TIME_ORDER:
                    if (configuration.contains(STREAMING_SOURCE_CONSUME_START_OFFSET)) {
                        String consumeOffsetStr =
                                configuration.getString(STREAMING_SOURCE_CONSUME_START_OFFSET);
                        consumeStartOffset = (T) Long.valueOf(toMills(consumeOffsetStr));
                    } else {
                        consumeStartOffset = (T) DEFAULT_MIN_TIME_OFFSET;
                    }
                    typeSerializer = (TypeSerializer<T>) LongSerializer.INSTANCE;
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported consumer order: " + consumeOrder);
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

        /**
         * Get the partition modified time.
         *
         * <p>the time is the the folder/file modification time in filesystem when fetched in
         * create-time order, the time is extracted from partition name when fetched in
         * partition-time order, the time is partition create time in metaStore when fetched in
         * partition-name order.
         */
        public long getModificationTime(Partition partition, T partitionOffset) {
            switch (consumeOrder) {
                case PARTITION_NAME_ORDER:
                    // second to millisecond
                    return partition.getCreateTime() * 1_1000L;
                case PARTITION_TIME_ORDER:
                case CREATE_TIME_ORDER:
                    return (Long) partitionOffset;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported consumer order: " + consumeOrder);
            }
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
}
