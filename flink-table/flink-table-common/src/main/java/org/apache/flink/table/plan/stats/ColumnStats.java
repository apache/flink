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

import java.util.ArrayList;
import java.util.List;

/**
 * Column statistics.
 */
@PublicEvolving
public final class ColumnStats {

	/**
	 * number of distinct values.
	 */
	private final Long ndv;

	/**
	 * number of nulls.
	 */
	private final Long nullCount;

	/**
	 * average length of column values.
	 */
	private final Double avgLen;

	/**
	 * max length of column values.
	 */
	private final Integer maxLen;

	/**
	 * max value of column values.
	 */
	private final Number max;

	/**
	 * min value of column values.
	 */
	private final Number min;

	public ColumnStats(
		Long ndv,
		Long nullCount,
		Double avgLen,
		Integer maxLen,
		Number max,
		Number min) {
		this.ndv = ndv;
		this.nullCount = nullCount;
		this.avgLen = avgLen;
		this.maxLen = maxLen;
		this.max = max;
		this.min = min;
	}

	public Long getNdv() {
		return ndv;
	}

	public Long getNullCount() {
		return nullCount;
	}

	public Double getAvgLen() {
		return avgLen;
	}

	public Integer getMaxLen() {
		return maxLen;
	}

	public Number getMaxValue() {
		return max;
	}

	public Number getMinValue() {
		return min;
	}

	public String toString() {
		List<String> columnStats = new ArrayList<>();
		if (ndv != null) {
			columnStats.add("ndv=" + ndv);
		}
		if (nullCount != null) {
			columnStats.add("nullCount=" + nullCount);
		}
		if (avgLen != null) {
			columnStats.add("avgLen=" + avgLen);
		}
		if (maxLen != null) {
			columnStats.add("maxLen=" + maxLen);
		}
		if (max != null) {
			columnStats.add("max=" + max);
		}
		if (min != null) {
			columnStats.add("min=" + min);
		}
		String columnStatsStr = String.join(", ", columnStats);
		return "ColumnStats(" + columnStatsStr + ")";
	}

	/**
	 * Create a deep copy of "this" instance.
	 * @return a deep copy
	 */
	public ColumnStats copy() {
		return new ColumnStats(this.ndv, this.nullCount, this.avgLen, this.maxLen,
			this.max, this.min);
	}

}
