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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Fetcher to fetch the suitable partitions of a filesystem table.
 *
 * @param <P> The type to describe a partition.
 */

@Internal
@FunctionalInterface
public interface PartitionFetcher<P> extends Serializable {

	/**
	 * Fetch the suitable partitions, call this method should guarantee the fetcher has opened.
	 */
	List<P> fetch(Context<P> context) throws Exception;

	/**
	 * Context for fetch partitions, partition information is stored in hive meta store.
	 */
	interface Context<P> extends Serializable {

		/**
		 * open the resources of the Context, this method should first call before call other methods.
		 */
		void open() throws Exception;

		/**
		 * Get partition by file partition values.
		 */
		Optional<P> getPartition(List<String> partValues) throws Exception;

		/**
		 * Get list that contains partition with comparable object.
		 *
		 * <p>For 'create-time' and 'partition-time',the comparable object type is Long which
		 * represents time in milliseconds, for 'partition-name', the comparable object type is String
		 * which represents the partition names string.
		 */
		List<ComparablePartitionValue> getComparablePartitionValueList() throws Exception;

		/**
		 * close the resources of the Context, this method should call when the context do not need any more.
		 */
		void close() throws Exception;

		/**
		 * A comparable partition value that can compare order by using its comparator.
		 * @param <P> The type of partition value.
		 * @param <T> The tye of the partition offset, the partition offset is comparable.
		 */
		interface ComparablePartitionValue<P, T extends Comparable<T>> extends Serializable {

			/**
			 * Get the partition value.
			 */
			P getPartitionValue();

			/**
			 * Get the comparator.
			 * @return
			 */
			T getComparator();
		}
	}
}
