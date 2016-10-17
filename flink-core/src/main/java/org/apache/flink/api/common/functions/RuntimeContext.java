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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A RuntimeContext contains information about the context in which functions are executed. Each parallel instance
 * of the function will have a context through which it can access static contextual information (such as 
 * the current parallelism) and other constructs like accumulators and broadcast variables.
 * <p>
 * A function can, during runtime, obtain the RuntimeContext via a call to
 * {@link AbstractRichFunction#getRuntimeContext()}.
 */
@Public
public interface RuntimeContext extends KeyedStateStore {

	/**
	 * Returns the name of the task in which the UDF runs, as assigned during plan construction.
	 * 
	 * @return The name of the task in which the UDF runs.
	 */
	String getTaskName();

	/**
	 * Returns the metric group for this parallel subtask.
	 * 
	 * @return The metric group for this parallel subtask.
	 */
	@PublicEvolving
	MetricGroup getMetricGroup();

	/**
	 * Gets the parallelism with which the parallel task runs.
	 * 
	 * @return The parallelism with which the parallel task runs.
	 */
	int getNumberOfParallelSubtasks();

	/**
	 * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
	 * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
	 * 
	 * @return The index of the parallel subtask.
	 */
	int getIndexOfThisSubtask();

	/**
	 * Gets the attempt number of this parallel subtask. First attempt is numbered 0.
	 *
	 * @return Attempt number of the subtask.
	 */
	int getAttemptNumber();

	/**
	 * Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
	 * where 3 would be ({@link #getIndexOfThisSubtask()} + 1), and 6 would be
	 * {@link #getNumberOfParallelSubtasks()}.
	 *
	 * @return The name of the task, with subtask indicator.
	 */
	String getTaskNameWithSubtasks();

	/**
	 * Returns the {@link org.apache.flink.api.common.ExecutionConfig} for the currently executing
	 * job.
	 */
	ExecutionConfig getExecutionConfig();
	
	/**
	 * Gets the ClassLoader to load classes that were are not in system's classpath, but are part of the
	 * jar file of a user job.
	 * 
	 * @return The ClassLoader for user code classes.
	 */
	ClassLoader getUserCodeClassLoader();

	// --------------------------------------------------------------------------------------------

	/**
	 * Add this accumulator. Throws an exception if the accumulator already exists in the same Task.
	 * Note that the Accumulator name must have an unique name across the Flink job. Otherwise you will
	 * get an error when incompatible accumulators from different Tasks are combined at the JobManager
	 * upon job completion.
	 */
	<V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator);

	/**
	 * Get an existing accumulator object. The accumulator must have been added
	 * previously in this local runtime context.
	 * 
	 * Throws an exception if the accumulator does not exist or if the
	 * accumulator exists, but with different type.
	 */
	<V, A extends Serializable> Accumulator<V, A> getAccumulator(String name);

	/**
	 * Returns a map of all registered accumulators for this task.
	 * The returned map must not be modified.
	 * @deprecated Use getAccumulator(..) to obtain the value of an accumulator.
	 */
	@Deprecated
	@PublicEvolving
	Map<String, Accumulator<?, ?>> getAllAccumulators();

	/**
	 * Convenience function to create a counter object for integers.
	 */
	@PublicEvolving
	IntCounter getIntCounter(String name);

	/**
	 * Convenience function to create a counter object for longs.
	 */
	@PublicEvolving
	LongCounter getLongCounter(String name);

	/**
	 * Convenience function to create a counter object for doubles.
	 */
	@PublicEvolving
	DoubleCounter getDoubleCounter(String name);

	/**
	 * Convenience function to create a counter object for histograms.
	 */
	@PublicEvolving
	Histogram getHistogram(String name);
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Tests for the existence of the broadcast variable identified by the
	 * given {@code name}.
	 *
	 * @param name The name under which the broadcast variable is registered;
	 * @return Whether a broadcast variable exists for the given name.
	 */
	@PublicEvolving
	boolean hasBroadcastVariable(String name);

	/**
	 * Returns the result bound to the broadcast variable identified by the 
	 * given {@code name}.
	 * <p>
	 * IMPORTANT: The broadcast variable data structure is shared between the parallel
	 *            tasks on one machine. Any access that modifies its internal state needs to
	 *            be manually synchronized by the caller.
	 * 
	 * @param name The name under which the broadcast variable is registered;
	 * @return The broadcast variable, materialized as a list of elements.
	 */
	<RT> List<RT> getBroadcastVariable(String name);
	
	/**
	 * Returns the result bound to the broadcast variable identified by the 
	 * given {@code name}. The broadcast variable is returned as a shared data structure
	 * that is initialized with the given {@link BroadcastVariableInitializer}.
	 * <p>
	 * IMPORTANT: The broadcast variable data structure is shared between the parallel
	 *            tasks on one machine. Any access that modifies its internal state needs to
	 *            be manually synchronized by the caller.
	 * 
	 * @param name The name under which the broadcast variable is registered;
	 * @param initializer The initializer that creates the shared data structure of the broadcast
	 *                    variable from the sequence of elements.
	 * @return The broadcast variable, materialized as a list of elements.
	 */
	<T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer);

	/**
	 * Returns the {@link DistributedCache} to get the local temporary file copies of files otherwise not
	 * locally accessible.
	 * 
	 * @return The distributed cache of the worker executing this instance.
	 */
	DistributedCache getDistributedCache();
}
