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

import org.apache.flink.connector.file.table.BucketIdComputer;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.Arrays;
import java.util.List;

/** Hive's implementation for {@link BucketIdComputer}. */
public class HiveRowDataBucketIdComputer implements BucketIdComputer<Row> {

    private static final long serialVersionUID = 1L;

    private final int numberOfBuckets;
    private final DataType[] columnTypes;
    private final int[] bucketedColIndexes;
    private final HiveObjectConversion[] bucketColHiveObjectConversions;

    private transient ObjectInspector[] bucketColObjectInspectors;

    public HiveRowDataBucketIdComputer(
            int numberOfBuckets,
            HiveShim hiveShim,
            String[] columnNames,
            DataType[] columnTypes,
            String[] bucketColumns) {
        this.numberOfBuckets = numberOfBuckets;
        this.columnTypes = columnTypes;
        List<String> columnList = Arrays.asList(columnNames);
        this.bucketedColIndexes =
                Arrays.stream(bucketColumns).mapToInt(columnList::indexOf).toArray();
        this.bucketColHiveObjectConversions = new HiveObjectConversion[bucketColumns.length];
        for (int i = 0; i < bucketedColIndexes.length; i++) {
            DataType bucketColType = columnTypes[bucketedColIndexes[i]];
            bucketColHiveObjectConversions[i] =
                    HiveInspectors.getConversion(
                            HiveInspectors.getObjectInspector(bucketColType),
                            bucketColType.getLogicalType(),
                            hiveShim);
        }
    }

    private void initBucketColObjectInspector() {
        bucketColObjectInspectors = new ObjectInspector[bucketedColIndexes.length];
        for (int i = 0; i < bucketedColIndexes.length; i++) {
            DataType bucketColType = columnTypes[bucketedColIndexes[i]];
            ObjectInspector objectInspector = HiveInspectors.getObjectInspector(bucketColType);
            bucketColObjectInspectors[i] = objectInspector;
        }
    }

    @Override
    public int getBucketId(Row in) {
        // follow Hive's behavior to calculate bucket id
        if (bucketColObjectInspectors == null) {
            initBucketColObjectInspector();
        }
        Object[] bucketFields = new Object[bucketColObjectInspectors.length];
        for (int i = 0; i < bucketedColIndexes.length; i++) {
            Object field = in.getField(bucketedColIndexes[i]);
            bucketFields[i] = bucketColHiveObjectConversions[i].toHiveObject(field);
        }
        int hashCode =
                ObjectInspectorUtils.getBucketHashCode(bucketFields, bucketColObjectInspectors);

        return ObjectInspectorUtils.getBucketNumber(hashCode, numberOfBuckets);
    }

    @Override
    public String getBucketFilePrefix(int bucketId) {
        // The bucket file name prefix is following Hive conversion, Presto and Trino conversion to
        // make sure the bucket files written by Flink can be read by other engines.
        // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
        // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.
        return String.format("%05d_0_", bucketId);
    }
}
