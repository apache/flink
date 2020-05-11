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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Partition strategy for helping fetch hive partitioned table.
 */
@Internal
public interface PartitionDiscovery {

	/**
	 * Fetch partitions by previous timestamp (Including).
	 */
	List<Tuple2<Partition, Long>> fetchPartitions(Context context, long previousTimestamp) throws Exception;

	/**
	 * Context for fetch partitions, partition information is stored in hive meta store.
	 */
	interface Context {

		/**
		 * Partition keys of this table.
		 */
		List<String> partitionKeys();

		/**
		 * See {@link IMetaStoreClient#getPartition}.
		 */
		Optional<Partition> getPartition(List<String> partValues) throws TException;

		/**
		 * Hadoop filesystem.
		 */
		FileSystem fileSystem();

		/**
		 * Root location of table.
		 */
		Path tableLocation();

		/**
		 * Extract timestamp from partition.
		 */
		long extractTimestamp(
				List<String> partKeys,
				List<String> partValues,
				Supplier<Long> fileTime);
	}
}
