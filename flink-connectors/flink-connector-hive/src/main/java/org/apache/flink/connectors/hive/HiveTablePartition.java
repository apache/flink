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
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A class that describes a partition of a Hive table. And it represents the whole table if table is
 * not partitioned.
 */
@PublicEvolving
public class HiveTablePartition implements Serializable {

    private static final long serialVersionUID = 4145470177119940673L;

    /** Partition storage descriptor. */
    private final CachedSerializedValue<StorageDescriptor> storageDescriptor;

    /** The map of partition key names and their values. */
    private final Map<String, String> partitionSpec;

    // Table properties that should be used to initialize SerDe
    private final Properties tableProps;

    /**
     * Creates a HiveTablePartition to describe a hive table.
     *
     * @param storageDescriptor SD of the hive table
     * @param tableProps properties of the hive table
     */
    public HiveTablePartition(StorageDescriptor storageDescriptor, Properties tableProps) {
        this(storageDescriptor, new LinkedHashMap<>(), tableProps);
    }

    /**
     * Creates a HiveTablePartition to describe a hive table or partition.
     *
     * @param storageDescriptor SD of the hive table or partition
     * @param partitionSpec the spec for the hive partition, and should be empty if the
     *     HiveTablePartition is to describe a hive table
     * @param tableProps properties of the hive table or partition
     */
    public HiveTablePartition(
            StorageDescriptor storageDescriptor,
            Map<String, String> partitionSpec,
            Properties tableProps) {
        try {
            this.storageDescriptor =
                    new CachedSerializedValue<>(
                            checkNotNull(storageDescriptor, "storageDescriptor can not be null"));
        } catch (IOException e) {
            throw new FlinkHiveException("Failed to serialize StorageDescriptor", e);
        }
        this.partitionSpec = checkNotNull(partitionSpec, "partitionSpec can not be null");
        this.tableProps = checkNotNull(tableProps, "tableProps can not be null");
    }

    public StorageDescriptor getStorageDescriptor() {
        try {
            return storageDescriptor.deserializeValue();
        } catch (IOException | ClassNotFoundException e) {
            throw new FlinkHiveException("Failed to deserialize StorageDescriptor", e);
        }
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    public Properties getTableProps() {
        return tableProps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiveTablePartition that = (HiveTablePartition) o;
        return Objects.equals(getStorageDescriptor(), that.getStorageDescriptor())
                && Objects.equals(partitionSpec, that.partitionSpec)
                && Objects.equals(tableProps, that.tableProps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStorageDescriptor(), partitionSpec, tableProps);
    }

    @Override
    public String toString() {
        StorageDescriptor sd = getStorageDescriptor();
        return "HiveTablePartition{"
                + String.format("PartitionSpec=%s, ", partitionSpec)
                + String.format("Location=%s, ", sd.getLocation())
                + String.format("InputFormat=%s", sd.getInputFormat())
                + "}";
    }

    /**
     * Creates a HiveTablePartition to represent a hive table.
     *
     * @param hiveConf the HiveConf used to connect to HMS
     * @param hiveVersion the version of hive in use, if it's null the version will be automatically
     *     detected
     * @param dbName name of the database
     * @param tableName name of the table
     */
    public static HiveTablePartition ofTable(
            HiveConf hiveConf, @Nullable String hiveVersion, String dbName, String tableName) {
        HiveShim hiveShim = getHiveShim(hiveVersion);
        try (HiveMetastoreClientWrapper client =
                new HiveMetastoreClientWrapper(hiveConf, hiveShim)) {
            Table hiveTable = client.getTable(dbName, tableName);
            return new HiveTablePartition(
                    hiveTable.getSd(), HiveReflectionUtils.getTableMetadata(hiveShim, hiveTable));
        } catch (TException e) {
            throw new FlinkHiveException(
                    String.format(
                            "Failed to create HiveTablePartition for hive table %s.%s",
                            dbName, tableName),
                    e);
        }
    }

    /**
     * Creates a HiveTablePartition to represent a hive partition.
     *
     * @param hiveConf the HiveConf used to connect to HMS
     * @param hiveVersion the version of hive in use, if it's null the version will be automatically
     *     detected
     * @param dbName name of the database
     * @param tableName name of the table
     * @param partitionSpec map from each partition column to its value. The map should contain
     *     exactly all the partition columns and in the order in which the partition columns are
     *     defined
     */
    public static HiveTablePartition ofPartition(
            HiveConf hiveConf,
            @Nullable String hiveVersion,
            String dbName,
            String tableName,
            LinkedHashMap<String, String> partitionSpec) {
        HiveShim hiveShim = getHiveShim(hiveVersion);
        try (HiveMetastoreClientWrapper client =
                new HiveMetastoreClientWrapper(hiveConf, hiveShim)) {
            Table hiveTable = client.getTable(dbName, tableName);
            Partition hivePartition =
                    client.getPartition(dbName, tableName, new ArrayList<>(partitionSpec.values()));
            return new HiveTablePartition(
                    hivePartition.getSd(),
                    partitionSpec,
                    HiveReflectionUtils.getTableMetadata(hiveShim, hiveTable));
        } catch (TException e) {
            throw new FlinkHiveException(
                    String.format(
                            "Failed to create HiveTablePartition for partition %s of hive table %s.%s",
                            partitionSpec, dbName, tableName),
                    e);
        }
    }

    private static HiveShim getHiveShim(String hiveVersion) {
        hiveVersion = hiveVersion != null ? hiveVersion : HiveShimLoader.getHiveVersion();
        return HiveShimLoader.loadHiveShim(hiveVersion);
    }
}
