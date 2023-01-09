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
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.connectors.hive.util.JobConfUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters.LocalDateConverter;
import org.apache.flink.table.data.util.DataFormatConverters.LocalDateTimeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicFileEnumerator} implementation for hive source. It uses {@link
 * HiveSourceFileEnumerator#createInputSplits} to generate splits like HiveSourceFileEnumerator, but
 * only enumerates {@link HiveTablePartition}s that exist in the {@link DynamicFilteringData} if a
 * DynamicFilteringData is provided.
 */
public class HiveSourceDynamicFileEnumerator implements DynamicFileEnumerator {

    public static final Set<LogicalTypeRoot> SUPPORTED_TYPES;

    static {
        // Note: also modify #createRowData if the SUPPORTED_TYPES is modified.
        Set<LogicalTypeRoot> supportedTypes = new HashSet<>();
        supportedTypes.add(LogicalTypeRoot.TINYINT);
        supportedTypes.add(LogicalTypeRoot.SMALLINT);
        supportedTypes.add(LogicalTypeRoot.INTEGER);
        supportedTypes.add(LogicalTypeRoot.BIGINT);
        supportedTypes.add(LogicalTypeRoot.CHAR);
        supportedTypes.add(LogicalTypeRoot.VARCHAR);
        supportedTypes.add(LogicalTypeRoot.DATE);
        supportedTypes.add(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        SUPPORTED_TYPES = supportedTypes;
    }

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveSourceDynamicFileEnumerator.class);

    private final String table;
    private final List<String> dynamicFilterPartitionKeys;
    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> allPartitions;
    private final JobConf jobConf;
    private final String defaultPartitionName;
    private final HiveShim hiveShim;
    private transient List<HiveTablePartition> finalPartitions;

    public HiveSourceDynamicFileEnumerator(
            String table,
            List<String> dynamicFilterPartitionKeys,
            List<HiveTablePartition> allPartitions,
            String hiveVersion,
            JobConf jobConf) {
        this.table = checkNotNull(table);
        this.dynamicFilterPartitionKeys = checkNotNull(dynamicFilterPartitionKeys);
        this.allPartitions = checkNotNull(allPartitions);
        this.jobConf = checkNotNull(jobConf);
        this.defaultPartitionName = JobConfUtils.getDefaultPartitionName(jobConf);
        this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        this.finalPartitions = this.allPartitions;
    }

    public void setDynamicFilteringData(DynamicFilteringData data) {
        LOG.debug("Filtering partitions of table {} based on the data: {}", table, data);
        if (!data.isFiltering()) {
            finalPartitions = allPartitions;
            return;
        }
        finalPartitions = new ArrayList<>();
        RowType rowType = data.getRowType();
        Preconditions.checkArgument(rowType.getFieldCount() == dynamicFilterPartitionKeys.size());
        for (HiveTablePartition partition : allPartitions) {
            RowData partitionRow = createRowData(rowType, partition.getPartitionSpec());
            if (partitionRow != null && data.contains(partitionRow)) {
                finalPartitions.add(partition);
            }
        }
        LOG.info(
                "Dynamic filtering table {}, original partition number is {}, remaining partition number {}",
                table,
                allPartitions.size(),
                finalPartitions.size());
    }

    @VisibleForTesting
    RowData createRowData(RowType rowType, Map<String, String> partitionSpec) {
        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            String value = partitionSpec.get(dynamicFilterPartitionKeys.get(i));
            LogicalType fieldType = rowType.getTypeAt(i);
            Object convertedValue =
                    HivePartitionUtils.restorePartitionValueFromType(
                            hiveShim, value, rowType.getTypeAt(i), defaultPartitionName);
            // Note: also modify supported types if the switch is modified.
            switch (rowType.getTypeAt(i).getTypeRoot()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    // No further process is necessary.
                    break;
                case CHAR:
                case VARCHAR:
                    convertedValue = StringData.fromString((String) convertedValue);
                    break;
                case DATE:
                    convertedValue =
                            LocalDateConverter.INSTANCE.toInternal((LocalDate) convertedValue);
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    convertedValue =
                            new LocalDateTimeConverter(9)
                                    .toInternal((LocalDateTime) convertedValue);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type for dynamic filtering:" + rowType.getTypeAt(i));
            }
            if (!fieldType.isNullable() && convertedValue == null) {
                return null;
            }
            rowData.setField(i, convertedValue);
        }
        return rowData;
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(
                HiveSourceFileEnumerator.createInputSplits(
                        minDesiredSplits, finalPartitions, jobConf, false));
    }

    @VisibleForTesting
    List<HiveTablePartition> getFinalPartitions() {
        return finalPartitions;
    }

    /** A factory to create {@link HiveSourceDynamicFileEnumerator}. */
    public static class Provider implements DynamicFileEnumerator.Provider {

        private static final long serialVersionUID = 1L;

        private final String table;
        private final List<String> dynamicFilterPartitionKeys;
        // The binary HiveTablePartition list, serialize it manually at compile time to avoid
        // deserializing it in TaskManager during runtime.
        private final List<byte[]> partitionBytes;
        private final String hiveVersion;
        private final JobConfWrapper jobConfWrapper;

        public Provider(
                String table,
                List<String> dynamicFilterPartitionKeys,
                List<byte[]> partitionBytes,
                String hiveVersion,
                JobConfWrapper jobConfWrapper) {
            this.table = checkNotNull(table);
            this.dynamicFilterPartitionKeys = checkNotNull(dynamicFilterPartitionKeys);
            this.partitionBytes = checkNotNull(partitionBytes);
            this.hiveVersion = checkNotNull(hiveVersion);
            this.jobConfWrapper = checkNotNull(jobConfWrapper);
        }

        @Override
        public DynamicFileEnumerator create() {
            return new HiveSourceDynamicFileEnumerator(
                    table,
                    dynamicFilterPartitionKeys,
                    HivePartitionUtils.deserializeHiveTablePartition(partitionBytes),
                    hiveVersion,
                    jobConfWrapper.conf());
        }
    }
}
