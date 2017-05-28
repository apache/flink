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

package org.apache.flink.api.java.summarize;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Summary for a column of booleans.
 */
@PublicEvolving
public class BooleanColumnSummary extends ColumnSummary {

	private long trueCount;
	private long falseCount;
	private long nullCount;

	public BooleanColumnSummary(long trueCount, long falseCount, long nullCount) {
		this.trueCount = trueCount;
		this.falseCount = falseCount;
		this.nullCount = nullCount;
	}

	public long getTrueCount() {
		return trueCount;
	}

	public long getFalseCount() {
		return falseCount;
	}

	/**
	 * The number of non-null values in this column.
	 */
	@Override
	public long getNonNullCount() {
		return trueCount + falseCount;
	}

	public long getNullCount() {
		return nullCount;
	}

	@Override
	public String toString() {
		return "BooleanColumnSummary{" +
			"totalCount=" + getTotalCount() +
			", trueCount=" + trueCount +
			", falseCount=" + falseCount +
			", nullCount=" + nullCount +
			'}';
	}
}
