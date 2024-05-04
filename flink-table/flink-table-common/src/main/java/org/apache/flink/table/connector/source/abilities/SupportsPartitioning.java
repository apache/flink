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
import org.apache.flink.table.connector.source.partitioning.Partitioning;

/**
 * Enables {@link ScanTableSource} to discover source partitions and inform the optimizer
 * accordingly.
 *
 * <p>Partitions split the data stored in an external system into smaller portions that are
 * identified by one or more string-based partition keys.
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
 * <p>In the above case (data is partitioned w.r.t. region and month) the optimizer might utilize
 * this pre-partitioned data source to eliminate possible shuffle operation. For example, for a
 * query SELECT region, month, AVG(age) from MyTable GROUP BY region, month the optimizer takes the
 * advantage of pre-partitioned source and avoids partitioning the data w.r.t. [region,month]
 */
@PublicEvolving
public interface SupportsPartitioning {

    /** Returns the output data partitioning that this reader guarantees. */
    Partitioning outputPartitioning();

    /** Applies partitioned reading to the source operator. */
    void applyPartitionedRead();
}
