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
import org.apache.flink.api.common.typeinfo.TypeInformation;

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
	 * Add this accumulator. Throws an exception if the accumulator already exists.
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
	 * Gets the key/value state, which is only accessible if the function is executed on
	 * a KeyedStream. Upon calling {@link OperatorState#value()}, the key/value state will
	 * return the value bound to the key of the element currently processed by the function.
	 * Each operator may maintain multiple key/value states, addressed with different names.
	 *
	 * <p>Because the scope of each value is the key of the currently processed element,
	 * and the elements are distributed by the Flink runtime, the system can transparently
	 * scale out and redistribute the state and KeyedStream.
	 *
	 * <p>The following code example shows how to implement a continuous counter that counts
	 * how many times elements of a certain key occur, and emits an updated count for that
	 * element on each occurrence. 
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");     
	 *
	 * keyedStream.map(new RichMapFunction<MyType, Tuple2<MyType, Long>>() {
	 *
	 *     private State<Long> state;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getKeyValueState(Long.class, 0L);
	 *     }
	 *
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         long count = state.value();
	 *         state.update(value + 1);
	 *         return new Tuple2<>(value, count);
	 *     }
	 * });
	 *
	 * }</pre>
	 *
	 * <p>This method attempts to deduce the type information from the given type class. If the
	 * full type cannot be determined from the class (for example because of generic parameters),
	 * the TypeInformation object must be manually passed via 
	 * {@link #getKeyValueState(String, TypeInformation, Object)}. 
	 * 
	 * @param name The name of the key/value state.
	 * @param stateType The class of the type that is stored in the state. Used to generate
	 *                  serializers for managed memory and checkpointing.
	 * @param defaultState The default state value, returned when the state is accessed and
	 *                     no value has yet been set for the key. May be null.
	 * @param <S> The type of the state.
	 *
	 * @return The key/value state access.
	 *
	 * @throws UnsupportedOperationException Thrown, if no key/value state is available for the
	 *                                       function (function is not part os a KeyedStream).
	 */
	<S> OperatorState<S> getKeyValueState(String name, Class<S> stateType, S defaultState);

	/**
	 * Gets the key/value state, which is only accessible if the function is executed on
	 * a KeyedStream. Upon calling {@link OperatorState#value()}, the key/value state will
	 * return the value bound to the key of the element currently processed by the function.
	 * Each operator may maintain multiple key/value states, addressed with different names.
	 * 
	 * <p>Because the scope of each value is the key of the currently processed element,
	 * and the elements are distributed by the Flink runtime, the system can transparently
	 * scale out and redistribute the state and KeyedStream.
	 * 
	 * <p>The following code example shows how to implement a continuous counter that counts
	 * how many times elements of a certain key occur, and emits an updated count for that
	 * element on each occurrence. 
	 * 
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");     
	 * 
	 * keyedStream.map(new RichMapFunction<MyType, Tuple2<MyType, Long>>() {
	 * 
	 *     private State<Long> state;
	 *     
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getKeyValueState(Long.class, 0L);
	 *     }
	 *     
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         long count = state.value();
	 *         state.update(value + 1);
	 *         return new Tuple2<>(value, count);
	 *     }
	 * });
	 *     
	 * }</pre>
	 * 
	 * @param name The name of the key/value state.
	 * @param stateType The type information for the type that is stored in the state.
	 *                  Used to create serializers for managed memory and checkpoints.   
	 * @param defaultState The default state value, returned when the state is accessed and
	 *                     no value has yet been set for the key. May be null.
	 * @param <S> The type of the state.
	 *    
	 * @return The key/value state access.
	 * 
	 * @throws UnsupportedOperationException Thrown, if no key/value state is available for the
	 *                                       function (function is not part os a KeyedStream).
	 */
	<S> OperatorState<S> getKeyValueState(String name, TypeInformation<S> stateType, S defaultState);
}
