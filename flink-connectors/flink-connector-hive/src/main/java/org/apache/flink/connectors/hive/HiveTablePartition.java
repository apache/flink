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

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A class that describes a partition of a Hive table. And it represents the whole table if table is not partitioned.
 * Please note that the class is serializable because all its member variables are serializable.
 */
public class HiveTablePartition implements Serializable {

	private static final long serialVersionUID = 4145470177119940673L;

	/** Partition storage descriptor. */
	private final StorageDescriptor storageDescriptor;

	/** The map of partition key names and their values. */
	private final Map<String, Object> partitionSpec;

	public HiveTablePartition(StorageDescriptor storageDescriptor) {
		this(storageDescriptor, new LinkedHashMap<>());
	}

	public HiveTablePartition(StorageDescriptor storageDescriptor, Map<String, Object> partitionSpec) {
		this.storageDescriptor = checkNotNull(storageDescriptor, "storageDescriptor can not be null");
		this.partitionSpec = checkNotNull(partitionSpec, "partitionSpec can not be null");
	}

	public StorageDescriptor getStorageDescriptor() {
		return storageDescriptor;
	}

	public Map<String, Object> getPartitionSpec() {
		return partitionSpec;
	}
}
