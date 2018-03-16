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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.annotation.Internal;

/**
 * Generic interface for aggregation.
 *
 * @param <T> the type to be aggregated
 * @param <R> the result type of the aggregation
 */
@Internal
public interface Aggregator<T, R> extends java.io.Serializable {

	/** Add a value to the current aggregation. */
	void aggregate(T value);

	/**
	 * Combine two aggregations of the same type.
	 *
	 * <p>(Implementations will need to do an unchecked cast).
	 */
	void combine(Aggregator<T, R> otherSameType);

	/** Provide the final result of the aggregation. */
	R result();
}
