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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.ContinuousPartitionFetcher;
import org.apache.flink.connector.file.table.LimitableBulkFormat;
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionFetcher;
import org.apache.flink.connectors.hive.read.HiveInputFormat;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.connectors.hive.util.HiveConfUtils;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTableUtil;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.file.src.FileSource.DEFAULT_SPLIT_ASSIGNER;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_PARTITION_ORDER;
import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.checkAcidTable;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Builder to build {@link HiveSource} instances. */
@PublicEvolving
public class HiveSourceBuilder {

    private static final Duration DEFAULT_SCAN_MONITOR_INTERVAL = Duration.ofMinutes(1L);

    private final JobConf jobConf;
    private final ReadableConfig flinkConf;
    private final boolean fallbackMappedReader;

    private final ObjectPath tablePath;
    private final Map<String, String> tableOptions;
    private final List<String> partitionKeys;
    private final String hiveVersion;

    private final DataType physicalDataType;
    @Nullable private int[] projectedFields;

    private Long limit;
    private List<HiveTablePartition> partitions;
    private List<String> dynamicFilterPartitionKeys;

    /**
     * Creates a builder to read a hive table.
     *
     * @param jobConf holds hive and hadoop configurations
     * @param flinkConf holds flink configurations
     * @param hiveVersion the version of hive in use, if it's null the version will be automatically
     *     detected
     * @param dbName the name of the database the table belongs to
     * @param tableName the name of the table
     * @param tableOptions additional options needed to read the table, which take precedence over
     *     table properties stored in metastore
     */
    public HiveSourceBuilder(
            @Nonnull JobConf jobConf,
            @Nonnull ReadableConfig flinkConf,
            @Nullable String hiveVersion,
            @Nonnull String dbName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> tableOptions) {
        this.jobConf = jobConf;
        this.flinkConf = flinkConf;
        this.fallbackMappedReader = flinkConf.get(TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER);
        this.tablePath = new ObjectPath(dbName, tableName);
        this.hiveVersion = hiveVersion == null ? HiveShimLoader.getHiveVersion() : hiveVersion;
        HiveConf hiveConf = HiveConfUtils.create(jobConf);
        HiveShim hiveShim = HiveShimLoader.loadHiveShim(this.hiveVersion);
        try (HiveMetastoreClientWrapper client =
                new HiveMetastoreClientWrapper(hiveConf, hiveShim)) {
            Table hiveTable = client.getTable(dbName, tableName);
            this.physicalDataType =
                    HiveTableUtil.extractRowType(hiveConf, hiveTable, client, hiveShim);
            this.partitionKeys = HiveCatalog.getFieldNames(hiveTable.getPartitionKeys());
            this.tableOptions = new HashMap<>(hiveTable.getParameters());
            this.tableOptions.putAll(tableOptions);
        } catch (TException e) {
            throw new FlinkHiveException("Failed to get hive table", e);
        }
        validateScanConfigurations(this.tableOptions);
        checkAcidTable(this.tableOptions, tablePath);
        setFlinkConfigurationToJobConf();
    }

    /**
     * Creates a builder to read a hive table.
     *
     * @param jobConf holds hive and hadoop configurations
     * @param flinkConf holds flink configurations
     * @param hiveVersion the version of hive in use, if it's null the version will be automatically
     *     detected
     * @param tablePath path of the table to be read
     * @param catalogTable the table to be read
     */
    public HiveSourceBuilder(
            @Nonnull JobConf jobConf,
            @Nonnull ReadableConfig flinkConf,
            @Nonnull ObjectPath tablePath,
            @Nullable String hiveVersion,
            @Nonnull ResolvedCatalogTable catalogTable) {
        this.jobConf = jobConf;
        this.flinkConf = flinkConf;
        this.fallbackMappedReader = flinkConf.get(TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER);
        this.tablePath = tablePath;
        this.hiveVersion = hiveVersion == null ? HiveShimLoader.getHiveVersion() : hiveVersion;
        this.physicalDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        this.partitionKeys = catalogTable.getPartitionKeys();
        this.tableOptions = catalogTable.getOptions();
        validateScanConfigurations(tableOptions);
        checkAcidTable(tableOptions, tablePath);
        setFlinkConfigurationToJobConf();
    }

    /**
     * Builds HiveSource with default built-in BulkFormat that returns records in type of RowData.
     */
    public HiveSource<RowData> buildWithDefaultBulkFormat() {
        return buildWithBulkFormat(createDefaultBulkFormat());
    }

    /** Builds HiveSource with custom BulkFormat. */
    public <T> HiveSource<T> buildWithBulkFormat(BulkFormat<T, HiveSourceSplit> bulkFormat) {
        Configuration configuration = Configuration.fromMap(tableOptions);
        ContinuousEnumerationSettings continuousSourceSettings = null;
        ContinuousPartitionFetcher<Partition, ?> fetcher = null;
        HiveTableSource.HiveContinuousPartitionFetcherContext<?> fetcherContext = null;
        if (isStreamingSource()) {
            Preconditions.checkState(
                    partitions == null, "setPartitions shouldn't be called in streaming mode");
            if (partitionKeys.isEmpty()) {
                HiveOptions.PartitionOrder partitionOrder =
                        configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
                if (partitionOrder != HiveOptions.PartitionOrder.CREATE_TIME) {
                    throw new UnsupportedOperationException(
                            "Only '"
                                    + HiveOptions.PartitionOrder.CREATE_TIME
                                    + "' is supported for non partitioned table.");
                }
                // for non-partitioned table, we need to add the table to partitions because
                // HiveSourceFileEnumerator needs it to create new splits
                partitions =
                        Collections.singletonList(
                                HiveTablePartition.ofTable(
                                        HiveConfUtils.create(jobConf),
                                        hiveVersion,
                                        tablePath.getDatabaseName(),
                                        tablePath.getObjectName()));
            }

            Duration monitorInterval =
                    configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL) == null
                            ? DEFAULT_SCAN_MONITOR_INTERVAL
                            : configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);
            checkArgument(
                    !(monitorInterval.isNegative() || monitorInterval.isZero()),
                    "monitorInterval must be > 0");

            continuousSourceSettings = new ContinuousEnumerationSettings(monitorInterval);

            if (!partitionKeys.isEmpty()) {
                fetcher = new HiveContinuousPartitionFetcher();
                final String defaultPartitionName = JobConfUtils.getDefaultPartitionName(jobConf);
                fetcherContext =
                        new HiveTableSource.HiveContinuousPartitionFetcherContext(
                                tablePath,
                                HiveShimLoader.loadHiveShim(hiveVersion),
                                new JobConfWrapper(jobConf),
                                partitionKeys,
                                configuration,
                                defaultPartitionName);
            }
        } else if (partitions == null) {
            partitions =
                    HivePartitionUtils.getAllPartitions(
                            jobConf, hiveVersion, tablePath, partitionKeys, null);
        }

        FileSplitAssigner.Provider splitAssigner =
                continuousSourceSettings == null || partitionKeys.isEmpty()
                        ? DEFAULT_SPLIT_ASSIGNER
                        : SimpleSplitAssigner::new;
        List<byte[]> hiveTablePartitionBytes = Collections.emptyList();
        if (partitions != null) {
            // Serializing the HiveTablePartition list manually at compile time to avoid
            // deserializing it in TaskManager during runtime. The HiveTablePartition list is no
            // need for TM.
            hiveTablePartitionBytes = HivePartitionUtils.serializeHiveTablePartition(partitions);
        }

        return new HiveSource<>(
                new Path[1],
                new HiveSourceFileEnumerator.Provider(
                        hiveTablePartitionBytes, new JobConfWrapper(jobConf)),
                splitAssigner,
                bulkFormat,
                continuousSourceSettings,
                jobConf,
                tablePath,
                partitionKeys,
                hiveVersion,
                dynamicFilterPartitionKeys,
                hiveTablePartitionBytes,
                fetcher,
                fetcherContext);
    }

    /**
     * Sets the partitions to read in batch mode. By default, batch source reads all partitions in a
     * hive table.
     */
    public HiveSourceBuilder setPartitions(List<HiveTablePartition> partitions) {
        this.partitions = partitions;
        return this;
    }

    public HiveSourceBuilder setDynamicFilterPartitionKeys(
            List<String> dynamicFilterPartitionKeys) {
        this.dynamicFilterPartitionKeys = dynamicFilterPartitionKeys;
        return this;
    }

    /** Sets the maximum number of records this source should return. */
    public HiveSourceBuilder setLimit(Long limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Sets the indices of projected fields.
     *
     * @param projectedFields indices of the fields, starting from 0
     */
    public HiveSourceBuilder setProjectedFields(int[] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    private RowType getProducedRowType() {
        DataType dataType;
        if (projectedFields == null) {
            dataType = physicalDataType;
        } else {
            dataType = Projection.of(projectedFields).project(physicalDataType);
        }
        return (RowType) dataType.bridgedTo(RowData.class).getLogicalType();
    }

    private static void validateScanConfigurations(Map<String, String> configurations) {
        String partitionInclude =
                configurations.getOrDefault(
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.defaultValue());
        Preconditions.checkArgument(
                "all".equals(partitionInclude),
                String.format(
                        "The only supported '%s' is 'all' in hive table scan, but is '%s'",
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(), partitionInclude));
    }

    private void setFlinkConfigurationToJobConf() {
        int splitPartitionThreadNum =
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM);
        Preconditions.checkArgument(
                splitPartitionThreadNum >= 1,
                HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.key()
                        + " cannot be less than 1");
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.key(),
                String.valueOf(splitPartitionThreadNum));
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX.key(),
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX).toString());
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.key(),
                String.valueOf(
                        flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES).getBytes()));
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST.key(),
                String.valueOf(
                        flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST).getBytes()));
        int calPartitionSizeThreadNum =
                flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM);
        Preconditions.checkArgument(
                calPartitionSizeThreadNum >= 1,
                HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key()
                        + " cannot be less than 1");
        jobConf.set(
                HiveOptions.TABLE_EXEC_HIVE_CALCULATE_PARTITION_SIZE_THREAD_NUM.key(),
                String.valueOf(calPartitionSizeThreadNum));
    }

    private boolean isStreamingSource() {
        return Boolean.parseBoolean(
                tableOptions.getOrDefault(
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_ENABLE.defaultValue().toString()));
    }

    protected BulkFormat<RowData, HiveSourceSplit> createDefaultBulkFormat() {
        return LimitableBulkFormat.create(
                new HiveInputFormat(
                        new JobConfWrapper(jobConf),
                        partitionKeys,
                        DataType.getFieldNames(physicalDataType).toArray(new String[0]),
                        DataType.getFieldDataTypes(physicalDataType).toArray(new DataType[0]),
                        hiveVersion,
                        getProducedRowType(),
                        fallbackMappedReader),
                limit);
    }
}
