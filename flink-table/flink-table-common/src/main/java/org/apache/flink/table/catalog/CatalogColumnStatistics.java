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

import java.util.HashMap;
import java.util.Map;

/**
 * Column statistics a non-partitioned table or a partition of a partitioned table.
 */
public class CatalogColumnStatistics {

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

	private final Map<String, String> properties;

	public CatalogColumnStatistics(
		Long ndv,
		Long nullCount,
		Double avgLen,
		Integer maxLen,
		Number max,
		Number min) {
		this(ndv, nullCount, avgLen, maxLen, max, min, new HashMap<>());
	}

	public CatalogColumnStatistics(
		Long ndv,
		Long nullCount,
		Double avgLen,
		Integer maxLen,
		Number max,
		Number min,
		Map<String, String> properties) {
		this.ndv = ndv;
		this.nullCount = nullCount;
		this.avgLen = avgLen;
		this.maxLen = maxLen;
		this.max = max;
		this.min = min;
		this.properties = properties;
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

	public Map<String, String> getProperties() {
		return this.properties;
	}

	/**
	 * Create a deep copy of "this" instance.
	 * @return a deep copy
	 */
	public CatalogColumnStatistics copy() {
		return new CatalogColumnStatistics(this.ndv, this.nullCount, this.avgLen, this.maxLen,
			this.max, this.min, new HashMap<>(this.properties));
	}

}
