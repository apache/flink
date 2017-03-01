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
package org.apache.flink.table.plan.nodes.datastream.aggs;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;

public interface StreamAggregator<T, R> extends Aggregator<T, R> {

	/**
	 * Some types of aggregations need to be recomputed along window element
	 * whereas other can just rely on element eviction policy. The reset method
	 * sets the aggregation value to its initial status, before any aggregation 
	 * was executed.
	 */
	void reset();
	
	/**
	 * When any type of sliding window purges elements, the aggregation function
	 * can update the result by invoking this method and implementing the necessary business logic.
	 * E.g. a sum aggregation in a window can subtract from the sum the evicted value. 
	 * @param evictedValue
	 */
	void evictElement(T evictedValue);
	
}
