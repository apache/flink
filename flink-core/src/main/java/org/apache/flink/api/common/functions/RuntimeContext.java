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

package org.apache.flink.api.common.functions;

import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;

/**
 * A RuntimeContext contains information about the context in which functions are executed. Each parallel instance
 * of the function will have a context through which it can access static contextual information (such as 
 * the current degree of parallelism) and other constructs like accumulators and broadcast variables.
 * <p>
 * A function can, during runtime, obtain the RuntimeContext via a call to
 * {@link AbstractRichFunction#getRuntimeContext()}.
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
	
	/**
	 * Gets the ClassLoader to load classes that were are not in system's classpath, but are part of the
	 * jar file of a user job.
	 * 
	 * @return The ClassLoader for user code classes.
	 */
	ClassLoader getUserCodeClassLoader();
	
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
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the result bound to the broadcast variable identified by the 
	 * given {@code name}.
	 */
	<RT> List<RT> getBroadcastVariable(String name);

	/**
	 * Returns the distributed cache to get the local tmp file.
	 */
	DistributedCache getDistributedCache();
}
