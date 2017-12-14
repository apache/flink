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
 * Summary for a column of values.
 */
@PublicEvolving
public abstract class ColumnSummary {

	/**
	 * The number of all rows in this column including both nulls and non-nulls.
	 */
	public long getTotalCount() {
		return getNullCount() + getNonNullCount();
	}

	/**
	 * The number of non-null values in this column.
	 */
	public abstract long getNonNullCount();

	/**
	 * The number of null values in this column.
	 */
	public abstract long getNullCount();

	/**
	 * True if this column contains any null values.
	 */
	public boolean containsNull() {
		return getNullCount() > 0L;
	}

	/**
	 * True if this column contains any non-null values.
	 */
	public boolean containsNonNull() {
		return getNonNullCount() > 0L;
	}

}
