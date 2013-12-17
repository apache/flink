/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.functions;

import java.util.HashMap;

import eu.stratosphere.api.accumulators.Accumulator;
import eu.stratosphere.api.accumulators.DoubleCounter;
import eu.stratosphere.api.accumulators.Histogram;
import eu.stratosphere.api.accumulators.IntCounter;
import eu.stratosphere.api.accumulators.LongCounter;

/**
 *
 */
public interface RuntimeContext {

	String getTaskName();

	int getNumberOfParallelSubtasks();

	int getIndexOfThisSubtask();

	/**
	 * Add this accumulator. Throws an exception if the counter is already
	 * existing.
	 * 
	 * This is only needed to support generic accumulators (e.g. for
	 * Set<String>). Didn't find a way to get this work with getAccumulator.
	 */
	<V, A> void addAccumulator(String name, Accumulator<V, A> accumulator);

	/**
	 * Get an existing accumulator object. The accumulator must have been added
	 * previously in this local runtime context.
	 * 
	 * Throws an exception if the accumulator does not exist or if the
	 * accumulator exists, but with different type.
	 */
	<V, A> Accumulator<V, A> getAccumulator(String name);

	/**
	 * For system internal usage only. Use getAccumulator(...) to obtain a
	 * accumulator. Use this as read-only.
	 */
	HashMap<String, Accumulator<?, ?>> getAllAccumulators();

	/**
	 * Convenience function to create a counter object for integers. This
	 * creates an accumulator object for double values internally.
	 * 
	 * @param name
	 * @return
	 */
	IntCounter getIntCounter(String name);

	LongCounter getLongCounter(String name);

	DoubleCounter getDoubleCounter(String name);

	Histogram getHistogram(String name);

//	/**
//	 * I propose to remove this and only keep the other more explicit functions
//	 * (to add or get an accumulator object)
//	 * 
//	 * Get an existing or new named accumulator object. Use this function to get
//	 * an counter for an custom accumulator type. For the integrated
//	 * accumulators you better use convenience functions (e.g. getIntCounter).
//	 * 
//	 * There is no need to register accumulators - they will be created when a
//	 * UDF asks the first time for a counter that does not exist yet locally.
//	 * This implies that there can be conflicts when a counter is requested with
//	 * the same name but with different types, either in the same UDF or in
//	 * different. In the last case the conflict occurs during merging.
//	 * 
//	 * @param name
//	 * @param accumulatorClass
//	 *            If the accumulator was not created previously
//	 * @return
//	 */
//	<V, A> Accumulator<V, A> getAccumulator(String name,
//			Class<? extends Accumulator<V, A>> accumulatorClass);
//
//	/**
//	 * See getAccumulable
//	 */
//	<T> SimpleAccumulator<T> getSimpleAccumulator(String name,
//			Class<? extends SimpleAccumulator<T>> accumulatorClass);

}
