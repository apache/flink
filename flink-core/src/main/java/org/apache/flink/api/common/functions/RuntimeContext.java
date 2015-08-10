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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;

/**
 * A RuntimeContext contains information about the context in which functions are executed. Each parallel instance
 * of the function will have a context through which it can access static contextual information (such as 
 * the current parallelism) and other constructs like accumulators and broadcast variables.
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
	 * Add this accumulator. Throws an exception if the counter is already
	 * existing.
	 * 
	 * This is only needed to support generic accumulators (e.g. for
	 * Set<String>). Didn't find a way to get this work with getAccumulator.
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
	Map<String, Accumulator<?, ?>> getAllAccumulators();

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
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the {@link OperatorState} with the given name of the underlying
	 * operator instance, which can be used to store and update user state in a
	 * fault tolerant fashion. The state will be initialized by the provided
	 * default value, and the {@link StateCheckpointer} will be used to draw the
	 * state snapshots.
	 * 
	 * <p>
	 * When storing a {@link Serializable} state the user can omit the
	 * {@link StateCheckpointer} in which case the full state will be written as
	 * the snapshot.
	 * </p>
	 * 
	 * @param name
	 *            Identifier for the state allowing that more operator states
	 *            can be used by the same operator.
	 * @param defaultState
	 *            Default value for the operator state. This will be returned
	 *            the first time {@link OperatorState#value()} (for every
	 *            state partition) is called before
	 *            {@link OperatorState#update(Object)}.
	 * @param partitioned
	 *            Sets whether partitioning should be applied for the given
	 *            state. If true a partitioner key must be used.
	 * @param checkpointer
	 *            The {@link StateCheckpointer} that will be used to draw
	 *            snapshots from the user state.
	 * @return The {@link OperatorState} for the underlying operator.
	 * 
	 * @throws IOException Thrown if the system cannot access the state.
	 */
	<S, C extends Serializable> OperatorState<S> getOperatorState(String name, S defaultState,
			boolean partitioned, StateCheckpointer<S, C> checkpointer) throws IOException;

	/**
	 * Returns the {@link OperatorState} with the given name of the underlying
	 * operator instance, which can be used to store and update user state in a
	 * fault tolerant fashion. The state will be initialized by the provided
	 * default value.
	 * 
	 * <p>
	 * When storing a non-{@link Serializable} state the user needs to specify a
	 * {@link StateCheckpointer} for drawing snapshots.
	 * </p>
	 * 
	 * @param name
	 *            Identifier for the state allowing that more operator states
	 *            can be used by the same operator.
	 * @param defaultState
	 *            Default value for the operator state. This will be returned
	 *            the first time {@link OperatorState#value()} (for every
	 *            state partition) is called before
	 *            {@link OperatorState#update(Object)}.
	 * @param partitioned
	 *            Sets whether partitioning should be applied for the given
	 *            state. If true a partitioner key must be used.
	 * @return The {@link OperatorState} for the underlying operator.
	 * 
	 * @throws IOException Thrown if the system cannot access the state.
	 */
	<S extends Serializable> OperatorState<S> getOperatorState(String name, S defaultState,
			boolean partitioned) throws IOException;
}
