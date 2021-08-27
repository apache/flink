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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.ScanTableSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Enables to pass available partitions to the planner and push down partitions into a {@link
 * ScanTableSource}.
 *
 * <p>Partitions split the data stored in an external system into smaller portions that are
 * identified by one or more string-based partition keys. A single partition is represented as a
 * {@code Map < String, String >} which maps each partition key to a partition value. Partition keys
 * and their order is defined by the catalog table.
 *
 * <p>For example, data can be partitioned by region and within a region partitioned by month. The
 * order of the partition keys (in the example: first by region then by month) is defined by the
 * catalog table. A list of partitions could be:
 *
 * <pre>
 *   List(
 *     ['region'='europe', 'month'='2020-01'],
 *     ['region'='europe', 'month'='2020-02'],
 *     ['region'='asia', 'month'='2020-01'],
 *     ['region'='asia', 'month'='2020-02']
 *   )
 * </pre>
 *
 * <p>By default, if this interface is not implemented, the data is read entirely with a subsequent
 * filter operation after the source.
 *
 * <p>For efficiency, the planner can pass the number of required partitions and a source must
 * exclude those partitions from reading (including reading the metadata). See {@link
 * #applyPartitions(List)}.
 *
 * <p>By default, the list of all partitions is queried from the catalog if necessary. However,
 * depending on the external system, it might be necessary to query the list of partitions in a
 * connector-specific way instead of using the catalog information. See {@link #listPartitions()}.
 *
 * <p>Note: After partitions are pushed into the source, the runtime will not perform a subsequent
 * filter operation for partition keys.
 */
@PublicEvolving
public interface SupportsPartitionPushDown {

    /**
     * Returns a list of all partitions that a source can read if available.
     *
     * <p>A single partition maps each partition key to a partition value.
     *
     * <p>If {@link Optional#empty()} is returned, the list of partitions is queried from the
     * catalog.
     */
    Optional<List<Map<String, String>>> listPartitions();

    /**
     * Provides a list of remaining partitions. After those partitions are applied, a source must
     * not read the data of other partitions during runtime.
     *
     * <p>See the documentation of {@link SupportsPartitionPushDown} for more information.
     */
    void applyPartitions(List<Map<String, String>> remainingPartitions);
}
