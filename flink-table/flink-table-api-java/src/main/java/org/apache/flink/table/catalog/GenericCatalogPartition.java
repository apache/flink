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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generic catalog partition implementation.
 */
public class GenericCatalogPartition implements CatalogPartition {
	private final PartitionSpec partitionSpec;
	private final Map<String, String> properties;

	private String comment = "This is a generic catalog partition";

	public GenericCatalogPartition(PartitionSpec partitionSpec, Map<String, String> properties) {
		this.partitionSpec = partitionSpec;
		this.properties = properties;
	}

	public GenericCatalogPartition(PartitionSpec partitionSpec, Map<String, String> properties, String comment) {
		this(partitionSpec, properties);
		this.comment = comment;
	}

	@Override
	public PartitionSpec getPartitionSpec() {
		return partitionSpec;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public CatalogPartition copy() {
		return new GenericCatalogPartition(partitionSpec, new HashMap<>(properties));
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(comment);
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of(comment);
	}

	/**
	 * A generic catalog partition spec implementation.
	 */
	public static class GenericPartitionSpec implements PartitionSpec {
		// <partition key, value>
		private final Map<String, String> partitionSpec;

		public GenericPartitionSpec(Map<String, String> partitionSpec) {
			checkNotNull(partitionSpec, "partitionSpec cannot be null");

			this.partitionSpec = Collections.unmodifiableMap(partitionSpec);
		}

		@Override
		public boolean contains(PartitionSpec another) {
			checkNotNull(another, "another cannot be null");

			return partitionSpec.entrySet().containsAll(((GenericPartitionSpec) another).partitionSpec.entrySet());
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			GenericPartitionSpec that = (GenericPartitionSpec) o;
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
}
