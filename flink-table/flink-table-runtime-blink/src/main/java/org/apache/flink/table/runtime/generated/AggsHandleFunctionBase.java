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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;

/**
 * The base class for handling aggregate or table aggregate functions.
 *
 * <p>It is code generated to handle all {@link AggregateFunction}s and
 * {@link TableAggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link AggregateFunction}s and
 * {@link TableAggregateFunction}s.
 */
public interface AggsHandleFunctionBase extends Function {

	/**
	 * Initialization method for the function. It is called before the actual working methods.
	 */
	void open(StateDataViewStore store) throws Exception;

	/**
	 * Accumulates the input values to the accumulators.
	 * @param input input values bundled in a row
	 */
	void accumulate(BaseRow input) throws Exception;

	/**
	 * Retracts the input values from the accumulators.
	 * @param input input values bundled in a row
	 */
	void retract(BaseRow input) throws Exception;

	/**
	 * Merges the other accumulators into current accumulators.
	 *
	 * @param accumulators The other row of accumulators
	 */
	void merge(BaseRow accumulators) throws Exception;

	/**
	 * Set the current accumulators (saved in a row) which contains the current aggregated results.
	 * In streaming: accumulators are store in the state, we need to restore aggregate buffers from state.
	 * In batch: accumulators are store in the hashMap, we need to restore aggregate buffers from hashMap.
	 *
	 * @param accumulators current accumulators
	 */
	void setAccumulators(BaseRow accumulators) throws Exception;

	/**
	 * Resets all the accumulators.
	 */
	void resetAccumulators() throws Exception;

	/**
	 * Gets the current accumulators (saved in a row) which contains the current
	 * aggregated results.
	 * @return the current accumulators
	 */
	BaseRow getAccumulators() throws Exception;

	/**
	 * Initializes the accumulators and save them to a accumulators row.
	 *
	 * @return a row of accumulators which contains the aggregated results
	 */
	BaseRow createAccumulators() throws Exception;

	/**
	 * Cleanup for the retired accumulators state.
	 */
	void cleanup() throws Exception;

	/**
	 * Tear-down method for this function. It can be used for clean up work.
	 * By default, this method does nothing.
	 */
	void close() throws Exception;
}
