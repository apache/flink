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

@SuppressWarnings("rawtypes")
public class Aggregations {

	public static AggregationFunction min(int field) {
		return new MinAggregationFunction(field);
	}
	
	public static AggregationFunction max(int field) {
		return new MaxAggregationFunction(field);
	}
	
	public static AggregationFunction count() {
		return new CountAggregationFunction();
	}
	
	public static AggregationFunction sum(int field) {
		return new SumAggregationFunction(field);
	}
	
	public static AggregationFunction average(int field) {
		return new AverageAggregationFunction(field);
	}
	
	public static AggregationFunction key(int field) {
		return new KeySelectionAggregationFunction(field);
	}
	
}
