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

package org.apache.flink.table.plan.stats;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Table statistics.
 */
@PublicEvolving
public final class TableStats {

	/**
	 * Unknown definition for table stats:
	 * Unknown {@link #rowCount} is -1.
	 * Unknown {@link #colStats} is not exist in map.
	 */
	public static final TableStats UNKNOWN = new TableStats(-1, new HashMap<>());

	/**
	 * cardinality of table.
	 */
	private final long rowCount;

	/**
	 * colStats statistics of table columns.
	 */
	private final Map<String, ColumnStats> colStats;

	public TableStats(long rowCount) {
		this(rowCount, new HashMap<>());
	}

	public TableStats(long rowCount, Map<String, ColumnStats> colStats) {
		this.rowCount = rowCount;
		this.colStats = colStats;
	}

	public long getRowCount() {
		return rowCount;
	}

	public Map<String, ColumnStats> getColumnStats() {
		return colStats;
	}

	/**
	 * Create a deep copy of "this" instance.
	 * @return a deep copy
	 */
	public TableStats copy() {
		TableStats copy = new TableStats(this.rowCount);
		for (Map.Entry<String, ColumnStats> entry : this.colStats.entrySet()) {
			copy.colStats.put(entry.getKey(), entry.getValue().copy());
		}
		return copy;
	}

	/**
	 * Merges two table stats.
	 * When the stats are unknown, whatever the other are, we need return unknown stats.
	 * See {@link #UNKNOWN}.
	 *
	 * @param other The other table stats to merge.
	 * @return The merged table stats.
	 */
	@Nonnull
	public TableStats merge(TableStats other) {
		Map<String, ColumnStats> colStats = new HashMap<>();
		for (Map.Entry<String, ColumnStats> entry : this.colStats.entrySet()) {
			String col = entry.getKey();
			ColumnStats stats = entry.getValue();
			ColumnStats otherStats = other.colStats.get(col);
			if (otherStats != null) {
				colStats.put(col, stats.merge(otherStats));
			}
		}
		return new TableStats(
				this.rowCount >= 0 && other.rowCount >= 0 ?
						this.rowCount + other.rowCount : UNKNOWN.rowCount,
				colStats);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableStats that = (TableStats) o;
		return rowCount == that.rowCount &&
				Objects.equals(colStats, that.colStats);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowCount, colStats);
	}

	@Override
	public String toString() {
		return "TableStats{" +
				"rowCount=" + rowCount +
				", colStats=" + colStats +
				'}';
	}
}
