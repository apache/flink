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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.sinks.TableSink;

import java.util.List;
import java.util.Map;

/**
 * An interface for partitionable {@link TableSource}.
 *
 * <p>A {@link PartitionableTableSource} can exclude partitions from reading, which
 * includes skipping the metadata. This is especially useful when there are thousands
 * of partitions in a table.
 *
 * <p>A partition is represented as a {@code Map<String, String>} which maps from partition
 * field name to partition value. Since the map is NOT ordered, the correct order of partition
 * fields should be obtained via {@link #getPartitionFieldNames()}.
 */
@Experimental
public interface PartitionableTableSource {

	/**
	 * Returns all the partitions of this {@link PartitionableTableSource}.
	 */
	List<Map<String, String>> getPartitions();

	/**
	 * Gets the partition field names of the table. The partition field names should be sorted in
	 * a strict order, i.e. they have the order as specified in the PARTITION statement in DDL.
	 * This should be an empty set if the table is not partitioned.
	 *
	 * <p>All the partition fields should exist in the {@link TableSink#getTableSchema()}.
	 *
	 * @return partition field names of the table, empty if the table is not partitioned.
	 */
	List<String> getPartitionFieldNames();

	/**
	 * Applies the remaining partitions to the table source. The {@code remainingPartitions} is
	 * the remaining partitions of {@link #getPartitions()} after partition pruning applied.
	 *
	 * <p>After trying to apply partition pruning, we should return a new {@link TableSource}
	 * instance which holds all pruned-partitions.
	 *
	 * @param remainingPartitions Remaining partitions after partition pruning applied.
	 * @return A new cloned instance of {@link TableSource} holds all pruned-partitions.
	 */
	TableSource applyPartitionPruning(List<Map<String, String>> remainingPartitions);
}
