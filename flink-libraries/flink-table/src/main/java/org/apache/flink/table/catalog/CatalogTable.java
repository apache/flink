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

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Represents a table in catalog.
 */
public class CatalogTable {
	// Table type, e.g csv, hbase, kafka
	private final String tableType;
	// Schema of the table (column names and types)
	private final TableSchema tableSchema;
	// Properties of the table
	private Map<String, String> properties = new HashMap<>();
	// RichTableSchema of the table
	private RichTableSchema richTableSchema;
	// Statistics of the table
	private TableStats tableStats = new TableStats();
	// Comment of the table
	private String comment;
	// Partitioned columns
	private LinkedHashSet<String> partitionColumnNames = new LinkedHashSet<>();
	// Whether the table is partitioned
	private boolean isPartitioned = false;
	// Computed columns expression
	private Map<String, RexNode> computedColumns;
	// Row time field
	private String rowTimeField = null;
	// Watermark offset for row time
	private long watermarkOffset = -1L;
	// Create timestamp of the table
	private long createTime = System.currentTimeMillis();
	// Timestamp of last access of the table
	private long lastAccessTime = -1L;
	// Flag whether this external table is intended for streaming or batch environments
	private final boolean isStreaming;

	public CatalogTable(
		String tableType,
		TableSchema tableSchema,
		TableStats tableStats,
		Map<String, String> properties,
		boolean isStreaming) {

		this.tableType = tableType;
		this.tableSchema = tableSchema;
		this.tableStats = tableStats;
		this.properties = properties;
		this.isStreaming = isStreaming;
		this.richTableSchema =
			new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
	}

	public CatalogTable(
		String tableType,
		TableSchema tableSchema,
		Map<String, String> properties,
		RichTableSchema richTableSchema,
		TableStats tableStats,
		String comment,
		LinkedHashSet<String> partitionColumnNames,
		boolean isPartitioned,
		Map<String, RexNode> computedColumns,
		String rowTimeField,
		long watermarkOffset,
		long createTime,
		long lastAccessTime,
		boolean isStreaming) {

		if (tableSchema != null && partitionColumnNames != null) {
			checkPartitionKeys(tableSchema, partitionColumnNames);
		}

		this.tableType = tableType;
		this.tableSchema = tableSchema;
		this.properties = properties;
		this.richTableSchema = richTableSchema;
		this.tableStats = tableStats;
		this.comment = comment;
		this.partitionColumnNames = partitionColumnNames;
		this.isPartitioned = isPartitioned;
		this.computedColumns = computedColumns;
		this.rowTimeField = rowTimeField;
		this.watermarkOffset = watermarkOffset;
		this.createTime = createTime;
		this.lastAccessTime = lastAccessTime;
		this.isStreaming = isStreaming;
	}

	private void checkPartitionKeys(TableSchema schema, LinkedHashSet<String> partitionColumnNames) {
		if (!partitionColumnNames.isEmpty()) {
			String[] colNames = schema.getFieldNames();
			String[] expectedPartitionCols = Arrays.copyOfRange(colNames, colNames.length - partitionColumnNames.size(), colNames.length);

			String[] partitionCols = new String[partitionColumnNames.size()];
			partitionColumnNames.toArray(partitionCols);

			if (!Arrays.equals(expectedPartitionCols, partitionCols)) {
				throw new IllegalArgumentException(
					String.format("Partition columns %s does not match the last %d columns of TableSchema %s",
						partitionColumnNames, partitionColumnNames.size(), tableSchema.getFieldNames())
				);
			}
		}
	}

	public String getTableType() {
		return tableType;
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public RichTableSchema getRichTableSchema() {
		return richTableSchema;
	}

	public TableStats getTableStats() {
		return tableStats;
	}

	public String getComment() {
		return comment;
	}

	public LinkedHashSet<String> getPartitionColumnNames() {
		return partitionColumnNames;
	}

	public boolean isPartitioned() {
		return isPartitioned;
	}

	public Map<String, RexNode> getComputedColumns() {
		return computedColumns;
	}

	public String getRowTimeField() {
		return rowTimeField;
	}

	public long getWatermarkOffset() {
		return watermarkOffset;
	}

	public long getCreateTime() {
		return createTime;
	}

	public long getLastAccessTime() {
		return lastAccessTime;
	}

	public boolean isStreaming() {
		return isStreaming;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CatalogTable that = (CatalogTable) o;

		return Objects.equals(tableType, that.tableType) &&
			Objects.equals(tableSchema, that.tableSchema) &&
			Objects.equals(tableStats, that.tableStats) &&
			isPartitioned == that.isPartitioned &&
			partitionColumnNames.equals(that.partitionColumnNames);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			tableType,
			tableSchema,
			tableStats,
			isPartitioned,
			partitionColumnNames);
	}

	@Override
	public String toString() {
		return "CatalogTable{" +
			"tableType='" + tableType + '\'' +
			", tableSchema=" + tableSchema +
			", tableStats=" + tableStats +
			", properties=" + properties +
			", richTableSchema=" + richTableSchema +
			", comment='" + comment + '\'' +
			", partitionColumnNames=" + partitionColumnNames +
			", isPartitioned=" + isPartitioned +
			", computedColumns=" + computedColumns +
			", rowTimeField='" + rowTimeField + '\'' +
			", watermarkOffset=" + watermarkOffset +
			", createTime=" + createTime +
			", lastAccessTime=" + lastAccessTime +
			", isStreaming=" + isStreaming +
			'}';
	}

	/**
	 * Builder class of CatalogTable.
	 * Note: do not add any additional fields to builder class.
	 */
	public static class Builder {
		private final String tableType;
		private final TableSchema tableSchema;
		private final boolean isStreaming;

		private TableStats tableStats = new TableStats();
		private Map<String, String> properties = new HashMap<>();

		public Builder(String tableType, TableSchema tableSchema, boolean isStreaming) {
			checkArgument(!StringUtils.isNullOrWhitespaceOnly(tableType), "tableType cannot be null or empty");
			checkNotNull(tableSchema, "tableSchema cannot be null or empty");

			this.tableType = tableType;
			this.tableSchema = tableSchema;
			this.isStreaming = isStreaming;
		}

		public Builder withTableStats(TableStats tableStats) {
			this.tableStats = checkNotNull(tableStats);
			return this;
		}

		public Builder withProperties(Map<String, String> properties) {
			this.properties = checkNotNull(properties);
			return this;
		}

		public CatalogTable build() {
			return new CatalogTable(
				tableType,
				tableSchema,
				tableStats,
				properties,
				isStreaming
			);
		}
	}
}
