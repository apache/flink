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
 * identified by partition keys.
 *
 * <p>For example, data can be partitioned by dt and within a dt partitioned by user_id.
 * the table definition could look like partition by (dt, bucket(user_id, 10)) and the partition
 * values can be ("2023-10-01", 0), ("2023-10-01", 1), ("2023-10-02", 0), ...
 *
 * <p>In the example above, the partition keys = [dt, bucket(user_id, 10)]. the optimizer might utilize
 * this pre-partitioned data source to eliminate possible shuffle operation.
 */
@PublicEvolving
public interface SupportsPartitioning {

    /** Returns the output data partitioning that this reader guarantees. */
    Partitioning outputPartitioning();

    /** Applies partitioned reading to the source operator. */
    void applyPartitionedRead();
}
