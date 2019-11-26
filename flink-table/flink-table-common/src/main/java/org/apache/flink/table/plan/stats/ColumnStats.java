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
	 * Deprecated because not well supported comparable type,
	 * e.g. {@link java.util.Date}, {@link java.sql.Timestamp}.
	 */
	@Deprecated
	private final Number maxValue;

	/**
	 * max value of column values, null if the value is unknown or not comparable.
	 */
	private final Comparable<?> max;

	/**
	 * Deprecated because not well supported comparable type,
	 * e.g. {@link java.util.Date}, {@link java.sql.Timestamp}.
	 */
	@Deprecated
	private final Number minValue;

	/**
	 * min value of column values, null if the value is unknown or not comparable.
	 */
	private final Comparable<?> min;

	/**
	 * Deprecated because Number type max/min is not well supported comparable type,
	 * e.g. {@link java.util.Date}, {@link java.sql.Timestamp}.
	 *  please use {@link ColumnStats.Builder} to construct ColumnStats instance.
	 */
	@Deprecated
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
		this.maxValue = max;
		this.minValue = min;
		this.max = null;
		this.min = null;
	}

	/**
	 * Private because to avoid "cannot resolve constructor" error.
	 * please use {@link ColumnStats.Builder} to construct ColumnStats instance.
	 * could change to public if the deprecated constructor is removed in the future.
	 */
	private ColumnStats(
			Long ndv,
			Long nullCount,
			Double avgLen,
			Integer maxLen,
			Comparable<?> max,
			Comparable<?> min) {
		this.ndv = ndv;
		this.nullCount = nullCount;
		this.avgLen = avgLen;
		this.maxLen = maxLen;
		this.max = max;
		this.min = min;
		this.maxValue = null;
		this.minValue = null;
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

	/**
	 * Deprecated because Number type max/min is not well supported comparable type,
	 * e.g. {@link java.util.Date}, {@link java.sql.Timestamp}.
	 *
	 * <p>Returns null if this instance is constructed by {@link ColumnStats.Builder}.
	 */
	@Deprecated
	public Number getMaxValue() {
		return maxValue;
	}

	/**
	 * Returns null if this instance is constructed by
	 * {@link ColumnStats#ColumnStats(Long, Long, Double, Integer, Number, Number)}.
	 */
	public Comparable<?> getMax() {
		return max;
	}

	/**
	 * Deprecated because Number type max/min is not well supported comparable type,
	 * e.g. {@link java.util.Date}, {@link java.sql.Timestamp}.
	 *
	 * <p>Returns null if this instance is constructed by {@link ColumnStats.Builder}.
	 */
	@Deprecated
	public Number getMinValue() {
		return minValue;
	}

	/**
	 * Returns null if this instance is constructed by
	 * {@link ColumnStats#ColumnStats(Long, Long, Double, Integer, Number, Number)}.
	 */
	public Comparable<?> getMin() {
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
		if (maxValue != null) {
			columnStats.add("max=" + maxValue);
		}
		if (min != null) {
			columnStats.add("min=" + min);
		}
		if (minValue != null) {
			columnStats.add("min=" + minValue);
		}
		String columnStatsStr = String.join(", ", columnStats);
		return "ColumnStats(" + columnStatsStr + ")";
	}

	/**
	 * Create a deep copy of "this" instance.
	 *
	 * @return a deep copy
	 */
	public ColumnStats copy() {
		if (maxValue != null || minValue != null) {
			return new ColumnStats(this.ndv, this.nullCount, this.avgLen, this.maxLen,
					this.maxValue, this.minValue);
		} else {
			return new ColumnStats(this.ndv, this.nullCount, this.avgLen, this.maxLen,
					this.max, this.min);
		}
	}

	/**
	 * ColumnStats builder.
	 */
	public static class Builder {
		private Long ndv = null;
		private Long nullCount = null;
		private Double avgLen = null;
		private Integer maxLen = null;
		private Comparable<?> max;
		private Comparable<?> min;

		public static Builder builder() {
			return new Builder();
		}

		public Builder setNdv(Long ndv) {
			this.ndv = ndv;
			return this;
		}

		public Builder setNullCount(Long nullCount) {
			this.nullCount = nullCount;
			return this;
		}

		public Builder setAvgLen(Double avgLen) {
			this.avgLen = avgLen;
			return this;
		}

		public Builder setMaxLen(Integer maxLen) {
			this.maxLen = maxLen;
			return this;
		}

		public Builder setMax(Comparable<?> max) {
			this.max = max;
			return this;
		}

		public Builder setMin(Comparable<?> min) {
			this.min = min;
			return this;
		}

		public ColumnStats build() {
			return new ColumnStats(ndv, nullCount, avgLen, maxLen, max, min);
		}
	}
}
