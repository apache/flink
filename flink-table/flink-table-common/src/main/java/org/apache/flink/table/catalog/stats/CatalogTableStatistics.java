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

package org.apache.flink.table.catalog.stats;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics for a non-partitioned table or a partition of a partitioned table.
 */
public class CatalogTableStatistics {
	public static final CatalogTableStatistics UNKNOWN = new CatalogTableStatistics(0, 0, 0, 0);

	/**
	 * The number of rows in the table or partition.
	 */
	private final long rowCount;

	/**
	 * The number of files on disk.
	 */
	private final int fileCount;

	/**
	 * The total size in bytes.
	 */
	private final long totalSize;

	/**
	 * The raw data size (size when loaded in memory) in bytes.
	 */
	private final long rawDataSize;

	private Map<String, String> properties;

	public CatalogTableStatistics(long rowCount, int fileCount, long totalSize, long rawDataSize) {
		this(rowCount, fileCount, totalSize, rawDataSize, new HashMap<>());
	}

	public CatalogTableStatistics(long rowCount, int fileCount, long totalSize, long rawDataSize,
			Map<String, String> properties) {
		this.rowCount = rowCount;
		this.fileCount = fileCount;
		this.totalSize = totalSize;
		this.rawDataSize = rawDataSize;
		this.properties = properties;
	}

	/**
	 * The number of rows.
	 */
	public long getRowCount() {
		return this.rowCount;
	}

	public int getFileCount() {
		return this.fileCount;
	}

	public long getTotalSize() {
		return this.totalSize;
	}

	public long getRawDataSize() {
		return this.rawDataSize;
	}

	public Map<String, String> getProperties() {
		return this.properties;
	}

	/**
	 * Create a deep copy of "this" instance.
	 *
	 * @return a deep copy
	 */
	public CatalogTableStatistics copy() {
		return new CatalogTableStatistics(this.rowCount, this.fileCount, this.totalSize, this.rawDataSize,
			new HashMap<>(this.properties));
	}

}
