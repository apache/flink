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

package org.apache.flink.connectors.hive.util;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Utils to load hive partitions from HiveMetaStore. */
public class HivePartitionUtils {

    /** Parse partition string specs into object values. */
    public static Map<String, Object> parsePartitionValues(
            Map<String, String> partitionSpecs,
            String[] fieldNames,
            DataType[] fieldTypes,
            String defaultPartitionName,
            HiveShim shim) {
        checkArgument(fieldNames.length == fieldTypes.length);
        List<String> fieldNameList = Arrays.asList(fieldNames);
        Map<String, Object> partitionColValues = new HashMap<>();
        for (Map.Entry<String, String> spec : partitionSpecs.entrySet()) {
            String partitionKey = spec.getKey();
            String valueString = spec.getValue();

            int index = fieldNameList.indexOf(partitionKey);
            if (index < 0) {
                throw new IllegalStateException(
                        String.format(
                                "Partition spec %s and column names %s doesn't match",
                                partitionSpecs, fieldNameList));
            }
            LogicalType partitionType = fieldTypes[index].getLogicalType();
            final Object value =
                    restorePartitionValueFromType(
                            shim, valueString, partitionType, defaultPartitionName);
            partitionColValues.put(partitionKey, value);
        }
        return partitionColValues;
    }

    public static Object restorePartitionValueFromType(
            HiveShim shim, String valStr, LogicalType partitionType, String defaultPartitionName) {
        if (defaultPartitionName.equals(valStr)) {
            if (LogicalTypeChecks.hasFamily(partitionType, LogicalTypeFamily.CHARACTER_STRING)) {
                // this keeps align with Hive,
                // maybe it should be null for string columns as well
                return defaultPartitionName;
            } else {
                return null;
            }
        }

        LogicalTypeRoot typeRoot = partitionType.getTypeRoot();
        // note: it's not a complete list ofr partition key types that Hive support, we may need add
        // more later.
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return valStr;
            case BOOLEAN:
                return Boolean.parseBoolean(valStr);
            case TINYINT:
                return Integer.valueOf(valStr).byteValue();
            case SMALLINT:
                return Short.valueOf(valStr);
            case INTEGER:
                return Integer.valueOf(valStr);
            case BIGINT:
                return Long.valueOf(valStr);
            case FLOAT:
                return Float.valueOf(valStr);
            case DOUBLE:
                return Double.valueOf(valStr);
            case DATE:
                return HiveInspectors.toFlinkObject(
                        HiveInspectors.getObjectInspector(partitionType),
                        shim.toHiveDate(Date.valueOf(valStr)),
                        shim);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return HiveInspectors.toFlinkObject(
                        HiveInspectors.getObjectInspector(partitionType),
                        shim.toHiveTimestamp(Timestamp.valueOf(valStr)),
                        shim);
            default:
                break;
        }
        throw new FlinkHiveException(
                new IllegalArgumentException(
                        String.format(
                                "Can not convert %s to type %s for partition value",
                                valStr, partitionType)));
    }

    /**
     * Returns all HiveTablePartitions of a hive table, returns single HiveTablePartition if the
     * hive table is not partitioned.
     */
    public static List<HiveTablePartition> getAllPartitions(
            JobConf jobConf,
            String hiveVersion,
            ObjectPath tablePath,
            List<String> partitionColNames,
            List<Map<String, String>> remainingPartitions) {
        List<HiveTablePartition> allHivePartitions = new ArrayList<>();
        try (HiveMetastoreClientWrapper client =
                HiveMetastoreClientFactory.create(HiveConfUtils.create(jobConf), hiveVersion)) {
            String dbName = tablePath.getDatabaseName();
            String tableName = tablePath.getObjectName();
            Table hiveTable = client.getTable(dbName, tableName);
            Properties tableProps =
                    HiveReflectionUtils.getTableMetadata(
                            HiveShimLoader.loadHiveShim(hiveVersion), hiveTable);
            if (partitionColNames != null && partitionColNames.size() > 0) {
                List<Partition> partitions = new ArrayList<>();
                if (remainingPartitions != null) {
                    for (Map<String, String> spec : remainingPartitions) {
                        partitions.add(
                                client.getPartition(
                                        dbName,
                                        tableName,
                                        partitionSpecToValues(spec, partitionColNames)));
                    }
                } else {
                    partitions.addAll(client.listPartitions(dbName, tableName, (short) -1));
                }
                for (Partition partition : partitions) {
                    HiveTablePartition hiveTablePartition =
                            toHiveTablePartition(partitionColNames, tableProps, partition);
                    allHivePartitions.add(hiveTablePartition);
                }
            } else {
                allHivePartitions.add(new HiveTablePartition(hiveTable.getSd(), tableProps));
            }
        } catch (TException e) {
            throw new FlinkHiveException("Failed to collect all partitions from hive metaStore", e);
        }
        return allHivePartitions;
    }

    public static List<String> partitionSpecToValues(
            Map<String, String> spec, List<String> partitionColNames) {
        checkArgument(
                spec.size() == partitionColNames.size()
                        && spec.keySet().containsAll(partitionColNames),
                "Partition spec (%s) and partition column names (%s) doesn't match",
                spec,
                partitionColNames);
        return partitionColNames.stream().map(spec::get).collect(Collectors.toList());
    }

    public static HiveTablePartition toHiveTablePartition(
            List<String> partitionKeys, Properties tableProps, Partition partition) {
        StorageDescriptor sd = partition.getSd();
        Map<String, String> partitionSpec = new HashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionColName = partitionKeys.get(i);
            String partitionValue = partition.getValues().get(i);
            partitionSpec.put(partitionColName, partitionValue);
        }
        return new HiveTablePartition(sd, partitionSpec, tableProps);
    }

    public static FileStatus[] getFileStatusRecurse(Path path, int expectLevel, FileSystem fs) {
        ArrayList<FileStatus> result = new ArrayList<>();

        try {
            FileStatus fileStatus = fs.getFileStatus(path);
            listStatusRecursively(fs, fileStatus, 0, expectLevel, result);
        } catch (IOException ignore) {
            return new FileStatus[0];
        }

        return result.toArray(new FileStatus[0]);
    }

    private static void listStatusRecursively(
            FileSystem fs,
            FileStatus fileStatus,
            int level,
            int expectLevel,
            List<FileStatus> results)
            throws IOException {
        if (expectLevel == level) {
            results.add(fileStatus);
            return;
        }

        if (fileStatus.isDir()) {
            for (FileStatus stat : fs.listStatus(fileStatus.getPath())) {
                listStatusRecursively(fs, stat, level + 1, expectLevel, results);
            }
        }
    }
}
