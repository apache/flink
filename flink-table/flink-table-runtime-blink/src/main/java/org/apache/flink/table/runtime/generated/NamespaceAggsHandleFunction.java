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
import org.apache.flink.table.runtime.dataview.StateDataViewStore;

/**
 * The base class for handling aggregate functions.
 *
 * <p>The differences between {@link NamespaceAggsHandleFunction} and {@link AggsHandleFunction}
 * is that the {@link NamespaceAggsHandleFunction} has namespace.
 * @param <N> type of namespace
 */
public interface NamespaceAggsHandleFunction<N> extends Function {

	/**
	 * Initialization method for the function. It is called before the actual working methods.
	 */
	void open(StateDataViewStore store) throws Exception;

	/**
	 * Set the current accumulators (saved in a row) which contains the current
	 * aggregated results.
	 * @param accumulators current accumulators
	 */
	void setAccumulators(N namespace, BaseRow accumulators) throws Exception;

	/**
	 * Accumulates the input values to the accumulators.
	 * @param inputRow input values bundled in a row
	 */
	void accumulate(BaseRow inputRow) throws Exception;

	/**
	 * Retracts the input values from the accumulators.
	 * @param inputRow input values bundled in a row
	 */
	void retract(BaseRow inputRow) throws Exception;

	/**
	 * Merges the other accumulators into current accumulators.
	 *
	 * @param otherAcc The other row of accumulators
	 */
	void merge(N namespace, BaseRow otherAcc) throws Exception;

	/**
	 * Initializes the accumulators and save them to a accumulators row.
	 *
	 * @return a row of accumulators which contains the aggregated results
	 */
	BaseRow createAccumulators() throws Exception;

	/**
	 * Gets the current accumulators (saved in a row) which contains the current
	 * aggregated results.
	 * @return the current accumulators
	 */
	BaseRow getAccumulators() throws Exception;

	/**
	 * Gets the result of the aggregation from the current accumulators and
	 * namespace properties (like window start).
	 * @param namespace the namespace properties which should be calculated, such window start
	 * @return the final result (saved in a row) of the current accumulators.
	 */
	BaseRow getValue(N namespace) throws Exception;

	/**
	 * Cleanup for the retired accumulators state.
	 */
	void cleanup(N namespace) throws Exception;

	/**
	 * Tear-down method for this function. It can be used for clean up work.
	 * By default, this method does nothing.
	 */
	void close() throws Exception;

}
