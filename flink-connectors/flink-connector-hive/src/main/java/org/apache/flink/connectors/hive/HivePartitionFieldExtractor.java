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

import org.apache.flink.connector.file.table.PartitionFieldExtractor;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.connectors.hive.read.HiveTableInputSplit;
import org.apache.flink.connectors.hive.util.HivePartitionUtils;
import org.apache.flink.table.catalog.hive.client.HiveShim;

/** Partition extractor to extract partition value from {@link HiveSourceSplit}. */
public class HivePartitionFieldExtractor {

    public static PartitionFieldExtractor<HiveSourceSplit> createForHiveSourceSplit(
            HiveShim hiveShim, String defaultPartitionName) {
        return (split, fieldName, fieldType) -> {
            String valueString = split.getHiveTablePartition().getPartitionSpec().get(fieldName);
            return HivePartitionUtils.restorePartitionValueFromType(
                    hiveShim, valueString, fieldType, defaultPartitionName);
        };
    }

    public static PartitionFieldExtractor<HiveTableInputSplit> createForHiveTableInputSplit(
            HiveShim hiveShim, String defaultPartitionName) {
        return (split, fieldName, fieldType) -> {
            String valueString = split.getHiveTablePartition().getPartitionSpec().get(fieldName);
            return HivePartitionUtils.restorePartitionValueFromType(
                    hiveShim, valueString, fieldType, defaultPartitionName);
        };
    }
}
