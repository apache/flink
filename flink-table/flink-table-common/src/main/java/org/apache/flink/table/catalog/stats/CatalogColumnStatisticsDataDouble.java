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
 * Column statistics value of double type.
 */
public class CatalogColumnStatisticsDataDouble extends CatalogColumnStatisticsDataBase {
	/**
	 * mim value.
	 */
	private final Double min;

	/**
	 * max value.
	 */
	private final Double max;

	/**
	 * number of distinct values.
	 */
	private final Long ndv;

	public CatalogColumnStatisticsDataDouble(Double min, Double max, Long ndv, Long nullCount) {
		super(nullCount);
		this.min = min;
		this.max = max;
		this.ndv = ndv;
	}

	public CatalogColumnStatisticsDataDouble(Double min, Double max, Long ndv, Long nullCount, Map<String, String> properties) {
		super(nullCount, properties);
		this.min = min;
		this.max = max;
		this.ndv = ndv;
	}

	public Double getMin() {
		return min;
	}

	public Double getMax() {
		return max;
	}

	public Long getNdv() {
		return ndv;
	}

	public CatalogColumnStatisticsDataDouble copy() {
		return new CatalogColumnStatisticsDataDouble(min, max, ndv, getNullCount(), new HashMap<>(getProperties()));
	}

}
