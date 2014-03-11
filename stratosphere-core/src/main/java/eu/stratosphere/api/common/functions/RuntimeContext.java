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
package eu.stratosphere.api.common.functions;

import java.util.Collection;
import java.util.HashMap;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.DoubleCounter;
import eu.stratosphere.api.common.accumulators.Histogram;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.common.cache.DistributedCache;

/**
 * A RuntimeContext contains information about the context in which functions are executed. Each parallel instance
 * of the function will have a context through which it can access static contextual information (such as 
 * the current degree of parallelism) and other constructs like accumulators and broadcast variables.
 * <p>
 * A function can, during runtime, obtain the RuntimeContext via a call to
 * {@link eu.stratosphere.api.common.functions.AbstractFunction#getRuntimeContext()}.
 */
public interface RuntimeContext {

	/**
	 * Returns the name of the task in which the UDF runs, as assigned during plan construction.
	 * 
	 * @return The name of the task in which the UDF runs.
	 */
	String getTaskName();

	/**
	 * Gets the degree of parallelism with which the parallel task runs.
	 * 
	 * @return The degree of parallelism with which the parallel task runs.
	 */
	int getNumberOfParallelSubtasks();

	/**
	 * Gets the number of the parallel subtask. The numbering starts from 1 and goes up to the degree-of-parallelism,
	 * as returned by {@link #getNumberOfParallelSubtasks()}.
	 * 
	 * @return The number of the parallel subtask.
	 */
	int getIndexOfThisSubtask();
	
	// --------------------------------------------------------------------------------------------

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
	 * Convenience function to create a counter object for integers.
	 */
	IntCounter getIntCounter(String name);

	/**
	 * Convenience function to create a counter object for longs.
	 */
	LongCounter getLongCounter(String name);

	/**
	 * Convenience function to create a counter object for doubles.
	 */
	DoubleCounter getDoubleCounter(String name);

	/**
	 * Convenience function to create a counter object for histograms.
	 */
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
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the result bound to the broadcast variable identified by the 
	 * given {@code name}.
	 */
	<RT> Collection<RT> getBroadcastVariable(String name);

	/**
	 * Returns the distributed cache to get the local tmp file.
	 */
	DistributedCache getDistributedCache();
}
