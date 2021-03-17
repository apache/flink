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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.ConsumeOrder;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.connectors.hive.util.HiveConfUtils;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_ORDER;
import static org.apache.flink.table.utils.PartitionPathUtils.extractPartitionValues;

/** Base class for table partition fetcher context. */
public abstract class HivePartitionFetcherContextBase<P> implements HivePartitionContext<P> {

    private static final long serialVersionUID = 1L;
    protected final ObjectPath tablePath;
    protected final HiveShim hiveShim;
    protected final JobConfWrapper confWrapper;
    protected final List<String> partitionKeys;
    protected final DataType[] fieldTypes;
    protected final String[] fieldNames;
    protected final Configuration configuration;
    protected final String defaultPartitionName;
    protected final ConsumeOrder consumeOrder;

    protected transient IMetaStoreClient metaStoreClient;
    protected transient StorageDescriptor tableSd;
    protected transient Properties tableProps;
    protected transient Path tableLocation;
    protected transient FileSystem fs;
    private transient PartitionTimeExtractor extractor;
    private transient Table table;

    public HivePartitionFetcherContextBase(
            ObjectPath tablePath,
            HiveShim hiveShim,
            JobConfWrapper confWrapper,
            List<String> partitionKeys,
            DataType[] fieldTypes,
            String[] fieldNames,
            Configuration configuration,
            String defaultPartitionName) {
        this.tablePath = tablePath;
        this.hiveShim = hiveShim;
        this.confWrapper = confWrapper;
        this.partitionKeys = partitionKeys;
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
        this.configuration = configuration;
        this.defaultPartitionName = defaultPartitionName;
        String consumeOrderStr = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
        this.consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
    }

    @Override
    public void open() throws Exception {
        metaStoreClient = hiveShim.getHiveMetastoreClient(HiveConfUtils.create(confWrapper.conf()));
        table = metaStoreClient.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
        tableSd = table.getSd();
        tableProps = HiveReflectionUtils.getTableMetadata(hiveShim, table);

        String extractorKind = configuration.get(PARTITION_TIME_EXTRACTOR_KIND);
        String extractorClass = configuration.get(PARTITION_TIME_EXTRACTOR_CLASS);
        String extractorPattern = configuration.get(PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);

        extractor =
                PartitionTimeExtractor.create(
                        Thread.currentThread().getContextClassLoader(),
                        extractorKind,
                        extractorClass,
                        extractorPattern);
        tableLocation = new Path(table.getSd().getLocation());
        fs = tableLocation.getFileSystem(confWrapper.conf());
    }

    @Override
    public List<ComparablePartitionValue> getComparablePartitionValueList() throws Exception {
        List<ComparablePartitionValue> partitionValueList = new ArrayList<>();
        switch (consumeOrder) {
            case PARTITION_NAME_ORDER:
                List<String> partitionNames =
                        metaStoreClient.listPartitionNames(
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                Short.MAX_VALUE);
                for (String partitionName : partitionNames) {
                    partitionValueList.add(getComparablePartitionByName(partitionName));
                }
                break;
            case CREATE_TIME_ORDER:
                FileStatus[] statuses =
                        HivePartitionUtils.getFileStatusRecurse(
                                tableLocation, partitionKeys.size(), fs);
                for (FileStatus status : statuses) {
                    List<String> partValues =
                            extractPartitionValues(
                                    new org.apache.flink.core.fs.Path(status.getPath().toString()));
                    Long creatTime =
                            TimestampData.fromTimestamp(new Timestamp(status.getModificationTime()))
                                    .getMillisecond();
                    partitionValueList.add(getComparablePartitionByTime(partValues, creatTime));
                }
                break;
            case PARTITION_TIME_ORDER:
                partitionNames =
                        metaStoreClient.listPartitionNames(
                                tablePath.getDatabaseName(),
                                tablePath.getObjectName(),
                                Short.MAX_VALUE);
                for (String partitionName : partitionNames) {
                    List<String> partValues =
                            extractPartitionValues(
                                    new org.apache.flink.core.fs.Path(partitionName));
                    Long partitionTime = toMills(extractor.extract(partitionKeys, partValues));
                    partitionValueList.add(getComparablePartitionByTime(partValues, partitionTime));
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported consumer order: " + consumeOrder);
        }
        return partitionValueList;
    }

    private ComparablePartitionValue<List<String>, Long> getComparablePartitionByTime(
            List<String> partValues, Long time) {
        return new ComparablePartitionValue<List<String>, Long>() {
            private static final long serialVersionUID = 1L;

            @Override
            public List<String> getPartitionValue() {
                return partValues;
            }

            @Override
            public Long getComparator() {
                // order by time (the time could be partition-time or create-time)
                return time;
            }
        };
    }

    private ComparablePartitionValue<List<String>, String> getComparablePartitionByName(
            String partitionName) {
        return new ComparablePartitionValue<List<String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public List<String> getPartitionValue() {
                return extractPartitionValues(new org.apache.flink.core.fs.Path(partitionName));
            }

            @Override
            public String getComparator() {
                // order by partition name in alphabetical order
                // partition name format: pt_year=2020/pt_month=10/pt_day=14
                return partitionName;
            }
        };
    }

    @Override
    public void close() throws Exception {
        if (this.metaStoreClient != null) {
            this.metaStoreClient.close();
        }
    }

    @Override
    public HiveTablePartition toHiveTablePartition(P partition) {
        return null;
    }
}
