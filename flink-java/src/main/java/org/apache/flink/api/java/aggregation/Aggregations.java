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

package org.apache.flink.api.java.aggregation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;

/**
 * Convenience factory to create {@link AggregationFunction}s.
 * 
 * @see DataSet
 * @see UnsortedGrouping
 */
@SuppressWarnings("rawtypes")
public class Aggregations {

	/**
	 * Compute the minimum value.
	 * @param field Tuple field index.
	 */
	public static AggregationFunction min(int field) {
		return new MinAggregationFunction(field);
	}
	
	/**
	 * Compute the maximum value.
	 * @param field Tuple field index.
	 */
	public static AggregationFunction max(int field) {
		return new MaxAggregationFunction(field);
	}
	
	/**
	 * Count the number of tuples.
	 */
	public static AggregationFunction count() {
		return new CountAggregationFunction();
	}
	
	/**
	 * Compute the sum.
	 * @param field Tuple field index.
	 */
	public static AggregationFunction sum(int field) {
		return new SumAggregationFunction(field);
	}
	
	/**
	 * Compute the average.
	 * @param field Tuple field index.
	 */
	public static AggregationFunction average(int field) {
		return new AverageAggregationFunction(field);
	}
	
	/**
	 * Select the field to be included in the grouping.
	 * @param field Tuple field index.
	 */
	public static AggregationFunction key(int field) {
		return new KeySelectionAggregationFunction(field);
	}
	
}
