/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.util;

/**
 * Collects the output of PACT first-order user function implemented as {@link Function}.
 * The collected data is forwards to the next contract.
 */
public interface Collector<T> {
	
	/**
	 * Emits a record from the invoking PACT first-order user function implemented as {@link Function}.
	 * 
	 * @param record The record to collect.
	 */
	void collect(T record);
	
	/**
	 * Closes the collector, flushing any buffered data.
	 */
	void close();
}
