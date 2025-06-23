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

package org.apache.flink.table.planner.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Helper functions for partitions. */
public class PartitionUtils {

    private PartitionUtils() {}

    /**
     * Returns partitions sorted by key.
     *
     * @param partitions list of partition key value pairs
     * @return sorted partitions
     */
    public static String sortPartitionsByKey(final List<Map<String, String>> partitions) {
        return partitions.stream()
                .map(PartitionUtils::sortPartitionByKey)
                .collect(Collectors.joining(", "));
    }

    private static String sortPartitionByKey(final Map<String, String> partition) {
        return partition.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
