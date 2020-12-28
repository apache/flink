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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveInputFormatPartitionReader;
import org.apache.flink.connectors.hive.read.HivePartitionFetcherContextBase;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.FileSystemLookupFunction;
import org.apache.flink.table.filesystem.PartitionFetcher;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.filesystem.FileSystemOptions.LOOKUP_JOIN_CACHE_TTL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_INCLUDE;

/**
 * Hive Table Source that has lookup ability.
 *
 * <p>Hive Table source has both lookup and continuous read ability, when it acts as continuous read
 * source it does not have the lookup ability but can be a temporal table just like other stream
 * sources. When it acts as bounded table, it has the lookup ability.
 *
 * <p>A common user case is use hive table as dimension table and always lookup the latest partition
 * data, in this case, hive table source is a continuous read source but currently we implements it
 * by LookupFunction. Because currently TableSource can not tell the downstream when the latest
 * partition has been read finished. This is a temporarily workaround and will re-implement in the
 * future.
 */
public class HiveLookupTableSource extends HiveTableSource implements LookupTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(HiveLookupTableSource.class);
    private static final Duration DEFAULT_LOOKUP_MONITOR_INTERVAL = Duration.ofHours(1L);
    private final Configuration configuration;
    private Duration hiveTableReloadInterval;

    public HiveLookupTableSource(
            JobConf jobConf,
            ReadableConfig flinkConf,
            ObjectPath tablePath,
            CatalogTable catalogTable) {
        super(jobConf, flinkConf, tablePath, catalogTable);
        this.configuration = new Configuration();
        catalogTable.getOptions().forEach(configuration::setString);
        validateLookupConfigurations();
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(getLookupFunction(context.getKeys()));
    }

    @VisibleForTesting
    TableFunction<RowData> getLookupFunction(int[][] keys) {
        int[] keyIndices = new int[keys.length];
        int i = 0;
        for (int[] key : keys) {
            if (key.length > 1) {
                throw new UnsupportedOperationException(
                        "Hive lookup can not support nested key now.");
            }
            keyIndices[i] = key[0];
            i++;
        }
        return getLookupFunction(keyIndices);
    }

    private void validateLookupConfigurations() {
        String partitionInclude = configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE);
        if (isStreamingSource()) {
            Preconditions.checkArgument(
                    !configuration.contains(STREAMING_SOURCE_CONSUME_START_OFFSET),
                    String.format(
                            "The '%s' is not supported when set '%s' to 'latest'",
                            STREAMING_SOURCE_CONSUME_START_OFFSET.key(),
                            STREAMING_SOURCE_PARTITION_INCLUDE.key()));

            Duration monitorInterval =
                    configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL) == null
                            ? DEFAULT_LOOKUP_MONITOR_INTERVAL
                            : configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);

            if (monitorInterval.toMillis() < DEFAULT_LOOKUP_MONITOR_INTERVAL.toMillis()) {
                LOG.warn(
                        String.format(
                                "Currently the recommended value of '%s' is at least '%s' when set '%s' to 'latest',"
                                        + " but actual is '%s', this may produce big pressure to hive metastore.",
                                STREAMING_SOURCE_MONITOR_INTERVAL.key(),
                                DEFAULT_LOOKUP_MONITOR_INTERVAL.toMillis(),
                                STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                                monitorInterval.toMillis()));
            }

            hiveTableReloadInterval = monitorInterval;
        } else {
            Preconditions.checkArgument(
                    "all".equals(partitionInclude),
                    String.format(
                            "The only supported %s for lookup is '%s' in batch source,"
                                    + " but actual is '%s'",
                            STREAMING_SOURCE_PARTITION_INCLUDE.key(), "all", partitionInclude));

            hiveTableReloadInterval = configuration.get(LOOKUP_JOIN_CACHE_TTL);
        }
    }

    private TableFunction<RowData> getLookupFunction(int[] keys) {

        final String defaultPartitionName =
                jobConf.get(
                        HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
                        HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
        PartitionFetcher.Context<HiveTablePartition> fetcherContext =
                new HiveTablePartitionFetcherContext(
                        tablePath,
                        hiveShim,
                        new JobConfWrapper(jobConf),
                        catalogTable.getPartitionKeys(),
                        getProducedTableSchema().getFieldDataTypes(),
                        getProducedTableSchema().getFieldNames(),
                        configuration,
                        defaultPartitionName);

        final PartitionFetcher<HiveTablePartition> partitionFetcher;
        // avoid lambda capture
        final ObjectPath tableFullPath = tablePath;
        if (catalogTable.getPartitionKeys().isEmpty()) {
            // non-partitioned table, the fetcher fetches the partition which represents the given
            // table.
            partitionFetcher =
                    context -> {
                        List<HiveTablePartition> partValueList = new ArrayList<>();
                        partValueList.add(
                                context.getPartition(new ArrayList<>())
                                        .orElseThrow(
                                                () ->
                                                        new IllegalArgumentException(
                                                                String.format(
                                                                        "Fetch partition fail for hive table %s.",
                                                                        tableFullPath))));
                        return partValueList;
                    };
        } else if (isStreamingSource()) {
            // streaming-read partitioned table, the fetcher fetches the latest partition of the
            // given table.
            partitionFetcher =
                    context -> {
                        List<HiveTablePartition> partValueList = new ArrayList<>();
                        List<PartitionFetcher.Context.ComparablePartitionValue>
                                comparablePartitionValues =
                                        context.getComparablePartitionValueList();
                        // fetch latest partitions for partitioned table
                        if (comparablePartitionValues.size() > 0) {
                            // sort in desc order
                            comparablePartitionValues.sort(
                                    (o1, o2) -> o2.getComparator().compareTo(o1.getComparator()));
                            PartitionFetcher.Context.ComparablePartitionValue maxPartition =
                                    comparablePartitionValues.get(0);
                            partValueList.add(
                                    context.getPartition(
                                                    (List<String>) maxPartition.getPartitionValue())
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalArgumentException(
                                                                    String.format(
                                                                            "Fetch partition fail for hive table %s.",
                                                                            tableFullPath))));
                        } else {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "At least one partition is required when set '%s' to 'latest' in temporal join,"
                                                    + " but actual partition number is '%s' for hive table %s",
                                            STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                                            comparablePartitionValues.size(),
                                            tableFullPath));
                        }
                        return partValueList;
                    };
        } else {
            // bounded-read partitioned table, the fetcher fetches all partitions of the given
            // filesystem table.
            partitionFetcher =
                    context -> {
                        List<HiveTablePartition> partValueList = new ArrayList<>();
                        List<PartitionFetcher.Context.ComparablePartitionValue>
                                comparablePartitionValues =
                                        context.getComparablePartitionValueList();
                        for (PartitionFetcher.Context.ComparablePartitionValue
                                comparablePartitionValue : comparablePartitionValues) {
                            partValueList.add(
                                    context.getPartition(
                                                    (List<String>)
                                                            comparablePartitionValue
                                                                    .getPartitionValue())
                                            .orElseThrow(
                                                    () ->
                                                            new IllegalArgumentException(
                                                                    String.format(
                                                                            "Fetch partition fail for hive table %s.",
                                                                            tableFullPath))));
                        }
                        return partValueList;
                    };
        }

        PartitionReader<HiveTablePartition, RowData> partitionReader =
                new HiveInputFormatPartitionReader(
                        jobConf,
                        hiveVersion,
                        tablePath,
                        getProducedTableSchema().getFieldDataTypes(),
                        getProducedTableSchema().getFieldNames(),
                        catalogTable.getPartitionKeys(),
                        projectedFields,
                        flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER));

        return new FileSystemLookupFunction<>(
                partitionFetcher,
                fetcherContext,
                partitionReader,
                (RowType) getProducedTableSchema().toRowDataType().getLogicalType(),
                keys,
                hiveTableReloadInterval);
    }

    /** PartitionFetcher.Context for {@link HiveTablePartition}. */
    static class HiveTablePartitionFetcherContext
            extends HivePartitionFetcherContextBase<HiveTablePartition> {

        private static final long serialVersionUID = 1L;

        public HiveTablePartitionFetcherContext(
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
        }

        @Override
        public Optional<HiveTablePartition> getPartition(List<String> partValues) throws Exception {
            Preconditions.checkArgument(
                    partitionKeys.size() == partValues.size(),
                    String.format(
                            "The partition keys length should equal to partition values length, "
                                    + "but partition keys length is %s and partition values length is %s",
                            partitionKeys.size(), partValues.size()));
            if (partitionKeys.isEmpty()) {
                // get partition that represents the non-partitioned table.
                return Optional.of(new HiveTablePartition(tableSd, tableProps));
            } else {
                try {
                    Partition partition =
                            metaStoreClient.getPartition(
                                    tablePath.getDatabaseName(),
                                    tablePath.getObjectName(),
                                    partValues);
                    HiveTablePartition hiveTablePartition =
                            HivePartitionUtils.toHiveTablePartition(
                                    partitionKeys,
                                    fieldNames,
                                    fieldTypes,
                                    hiveShim,
                                    tableProps,
                                    defaultPartitionName,
                                    partition);
                    return Optional.of(hiveTablePartition);
                } catch (NoSuchObjectException e) {
                    return Optional.empty();
                }
            }
        }
    }
}
