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

package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A class that describes a partition of a Hive table. And it represents the whole table if table is not partitioned.
 * Please note that the class is serializable because all its member variables are serializable.
 */
public class HiveTablePartition implements Serializable {

	private static final long serialVersionUID = 4145470177119940673L;

	/** Partition storage descriptor. */
	private final CachedSerializedValue<StorageDescriptor> storageDescriptor;

	/** The map of partition key names and their values. */
	private final Map<String, Object> partitionSpec;

	// Table properties that should be used to initialize SerDe
	private final Properties tableProps;

	public HiveTablePartition(StorageDescriptor storageDescriptor, Properties tableProps) {
		this(storageDescriptor, new LinkedHashMap<>(), tableProps);
	}

	public HiveTablePartition(StorageDescriptor storageDescriptor, Map<String, Object> partitionSpec, Properties tableProps) {
		try {
			this.storageDescriptor = new CachedSerializedValue<>(checkNotNull(storageDescriptor, "storageDescriptor can not be null"));
		} catch (IOException e) {
			throw new FlinkHiveException("Failed to serialize StorageDescriptor", e);
		}
		this.partitionSpec = checkNotNull(partitionSpec, "partitionSpec can not be null");
		this.tableProps = checkNotNull(tableProps, "tableProps can not be null");
	}

	public StorageDescriptor getStorageDescriptor() {
		try {
			return storageDescriptor.deserializeValue();
		} catch (IOException | ClassNotFoundException e) {
			throw new FlinkHiveException("Failed to deserialize StorageDescriptor", e);
		}
	}

	public Map<String, Object> getPartitionSpec() {
		return partitionSpec;
	}

	public Properties getTableProps() {
		return tableProps;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HiveTablePartition that = (HiveTablePartition) o;
		return Objects.equals(storageDescriptor, that.storageDescriptor) &&
				Objects.equals(partitionSpec, that.partitionSpec) &&
				Objects.equals(tableProps, that.tableProps);
	}

	@Override
	public int hashCode() {
		return Objects.hash(storageDescriptor, partitionSpec, tableProps);
	}

	@Override
	public String toString() {
		return "HiveTablePartition{" +
				"storageDescriptor=" + getStorageDescriptor() +
				", partitionSpec=" + partitionSpec +
				", tableProps=" + tableProps +
				'}';
	}
}
