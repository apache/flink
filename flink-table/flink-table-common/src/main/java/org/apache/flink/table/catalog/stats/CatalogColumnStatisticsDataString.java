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
 * Column statistics value of string type.
 */
public class CatalogColumnStatisticsDataString extends CatalogColumnStatisticsDataBase {
	/**
	 * max length of all values.
	 */
	private final long maxLength;

	/**
	 * average length of all values.
	 */
	private final double avgLength;

	/**
	 * number of distinct values.
	 */
	private final long ndv;

	public CatalogColumnStatisticsDataString(long maxLength, double avgLength, long ndv, long nullCount) {
		super(nullCount);
		this.maxLength = maxLength;
		this.avgLength = avgLength;
		this.ndv = ndv;
	}

	public CatalogColumnStatisticsDataString(long maxLength, double avgLength, long ndv, long nullCount, Map<String, String> properties) {
		super(nullCount, properties);
		this.maxLength = maxLength;
		this.avgLength = avgLength;
		this.ndv = ndv;
	}

	public long getMaxLength() {
		return maxLength;
	}

	public double getAvgLength() {
		return avgLength;
	}

	public long getNdv() {
		return ndv;
	}

	public CatalogColumnStatisticsDataString copy() {
		return new CatalogColumnStatisticsDataString(maxLength, avgLength, ndv, getNullCount(), new HashMap<>(getProperties()));
	}

}
