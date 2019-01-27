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

package org.apache.flink.table.catalog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Represents a partition object in catalog.
 */
public class CatalogPartition {
	private final PartitionSpec partitionSpec;
	private final Map<String, String> properties;

	public CatalogPartition(PartitionSpec partitionSpec, Map<String, String> properties) {
		this.partitionSpec = checkNotNull(partitionSpec);
		this.properties = checkNotNull(properties);
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public PartitionSpec getPartitionSpec() {
		return partitionSpec;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CatalogPartition that = (CatalogPartition) o;
		return partitionSpec.equals(that.partitionSpec);
	}

	@Override
	public int hashCode() {
		return Objects.hash(partitionSpec);
	}

	@Override
	public String toString() {
		return "CatalogPartition{" +
			"partitionSpec=" + partitionSpec +
			", properties=" + properties +
			'}';
	}

	/**
	 * Represents a partition spec object.
	 * Partition columns and values are NOT of strict order, and they need to be re-arranged to the correct order
	 * by comparing with a list of strictly ordered partition keys.
	 */
	public static class PartitionSpec {
		// <Column Name, Partition Value>
		private final Map<String, String> partitionSpec;

		public PartitionSpec(Map<String, String> partitionSpecs) {
			this.partitionSpec = Collections.unmodifiableMap(partitionSpecs);
		}

		/**
		 * Check if the current PartitionSpec contains all the partition columns and values of another PartitionSpec.
		 *
		 */
		public boolean contains(PartitionSpec that) {
			checkNotNull(that);

			return partitionSpec.entrySet().containsAll(that.partitionSpec.entrySet());
		}

		/**
		 * Get a list of ordered partition values by re-arranging them based on the given list of partition keys.
		 */
		public List<String> getOrderedValues(List<String> partitionKeys) {
			checkNotNull(partitionKeys, "partitionKeys cannot be null");

			int size = Math.min(partitionSpec.size(), partitionKeys.size());
			List<String> values = new ArrayList<>(size);

			for (int i = 0; i < size; i++) {
				String key = partitionKeys.get(i);

				if (!partitionSpec.containsKey(key)) {
					throw new IllegalArgumentException(
						String.format("PartitionSpec %s doesn't have key '%s' in the given partition key list %s",
							this.toString(), key, partitionKeys));
				}

				values.add(partitionSpec.get(key));
			}

			return values;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			PartitionSpec that = (PartitionSpec) o;
			return partitionSpec.equals(that.partitionSpec);
		}

		@Override
		public int hashCode() {
			return Objects.hash(partitionSpec);
		}

		@Override
		public String toString() {
			return "PartitionSpec{" + partitionSpec + '}';
		}
	}

	/**
	 * Create a PartitionSpec from a list of strings with format partition_col=partition_val.
	 * Example of input list - ["name=bob", "year=2019"]
	 */
	public static PartitionSpec fromStrings(List<String> partitionSpecStrings) {
		Map<String, String> map = new HashMap<>(partitionSpecStrings.size());

		for (String s : partitionSpecStrings) {
			String[] kv = s.split("=");
			map.put(kv[0], kv[1]);
		}

		return new PartitionSpec(map);
	}
}
