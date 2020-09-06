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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Represents a partition spec object in catalog.
 * Partition columns and values are NOT of strict order, and they need to be re-arranged to the correct order
 * by comparing with a list of strictly ordered partition keys.
 */
public class CatalogPartitionSpec {

	// An unmodifiable map as <partition key, value>
	private final Map<String, String> partitionSpec;

	public CatalogPartitionSpec(Map<String, String> partitionSpec) {
		checkNotNull(partitionSpec, "partitionSpec cannot be null");

		this.partitionSpec = Collections.unmodifiableMap(partitionSpec);
	}

	/**
	 * Get the partition spec as key-value map.
	 *
	 * @return a map of partition spec keys and values
	 */
	public Map<String, String> getPartitionSpec() {
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

		CatalogPartitionSpec that = (CatalogPartitionSpec) o;
		return partitionSpec.equals(that.partitionSpec);
	}

	@Override
	public int hashCode() {
		return Objects.hash(partitionSpec);
	}

	@Override
	public String toString() {
		return "CatalogPartitionSpec{" + partitionSpec + '}';
	}
}
