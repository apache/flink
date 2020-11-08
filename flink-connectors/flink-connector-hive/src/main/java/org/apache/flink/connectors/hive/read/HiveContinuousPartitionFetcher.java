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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;
import org.apache.flink.table.filesystem.PartitionFetcher;
import org.apache.flink.table.filesystem.PartitionFetcher.Context.ComparablePartitionValue;

import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ContinuousPartitionFetcher} for hive table.
 */
@Internal
public class HiveContinuousPartitionFetcher<T extends Comparable<T>> implements ContinuousPartitionFetcher<Partition, T> {

	private static final long serialVersionUID = 1L;

	@Override
	@SuppressWarnings("unchecked")
	public List<Tuple2<Partition, T>> fetchPartitions(Context<Partition, T> context, T previousOffset) throws Exception {
		List<Tuple2<Partition, T>> partitions = new ArrayList<>();
		List<ComparablePartitionValue> partitionValueList = context.getComparablePartitionValueList();
		for (ComparablePartitionValue<List<String>, T> partitionValue : partitionValueList) {
			T partitionOffset = partitionValue.getComparator();
			if (partitionOffset.compareTo(previousOffset) >= 0) {
				Partition partition = context
						.getPartition(partitionValue.getPartitionValue())
						.orElseThrow(() -> new IllegalArgumentException(
								String.format("Fetch partition fail for hive table %s.", context.getTablePath())));
				partitions.add(new Tuple2<>(partition, partitionValue.getComparator()));
			}
		}
		return partitions;
	}

	@Override
	public List<Partition> fetch(PartitionFetcher.Context<Partition> context) throws Exception {
		throw new UnsupportedOperationException("HiveContinuousPartitionFetcher does not support fetch all partition.");
	}

}
