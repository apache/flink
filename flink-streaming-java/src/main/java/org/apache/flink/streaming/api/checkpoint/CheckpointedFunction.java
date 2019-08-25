/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

/**
 * This is the core interface for <i>stateful transformation functions</i>, meaning functions
 * that maintain state across individual stream records.
 * While more lightweight interfaces exist as shortcuts for various types of state, this interface offer the
 * greatest flexibility in managing both <i>keyed state</i> and <i>operator state</i>.
 *
 * <p>The section <a href="#shortcuts">Shortcuts</a> illustrates the common lightweight
 * ways to setup stateful functions typically used instead of the full fledged
 * abstraction represented by this interface.
 *
 * <h1>Initialization</h1>
 * The {@link CheckpointedFunction#initializeState(FunctionInitializationContext)} is called when
 * the parallel instance of the transformation function is created during distributed execution.
 * The method gives access to the {@link FunctionInitializationContext} which in turn gives access
 * to the to the {@link OperatorStateStore} and {@link KeyedStateStore}.
 *
 * <p>The {@code OperatorStateStore} and {@code KeyedStateStore} give access to the data structures
 * in which state should be stored for Flink to transparently manage and checkpoint it, such as
 * {@link org.apache.flink.api.common.state.ValueState} or
 * {@link org.apache.flink.api.common.state.ListState}.
 *
 * <p><b>Note:</b> The {@code KeyedStateStore} can only be used when the transformation supports
 * <i>keyed state</i>, i.e., when it is applied on a keyed stream (after a {@code keyBy(...)}).
 *
 * <h1>Snapshot</h1>
 * The {@link CheckpointedFunction#snapshotState(FunctionSnapshotContext)} is called whenever a
 * checkpoint takes a state snapshot of the transformation function. Inside this method, functions typically
 * make sure that the checkpointed data structures (obtained in the initialization phase) are up
 * to date for a snapshot to be taken. The given snapshot context gives access to the metadata
 * of the checkpoint.
 *
 * <p>In addition, functions can use this method as a hook to flush/commit/synchronize with
 * external systems.
 *
 * <h1>Example</h1>
 * The code example below illustrates how to use this interface for a function that keeps counts
 * of events per key and per parallel partition (parallel instance of the transformation function
 * during distributed execution).
 * The example also changes of parallelism, which affect the count-per-parallel-partition by
 * adding up the counters of partitions that get merged on scale-down. Note that this is a
 * toy example, but should illustrate the basic skeleton for a stateful function.
 *
 * <p><pre>{@code
 * public class MyFunction<T> implements MapFunction<T, T>, CheckpointedFunction {
 *
 *     private ReducingState<Long> countPerKey;
 *     private ListState<Long> countPerPartition;
 *
 *     private long localCount;
 *
 *     public void initializeState(FunctionInitializationContext context) throws Exception {
 *         // get the state data structure for the per-key state
 *         countPerKey = context.getKeyedStateStore().getReducingState(
 *                 new ReducingStateDescriptor<>("perKeyCount", new AddFunction<>(), Long.class));
 *
 *         // get the state data structure for the per-partition state
 *         countPerPartition = context.getOperatorStateStore().getOperatorState(
 *                 new ListStateDescriptor<>("perPartitionCount", Long.class));
 *
 *         // initialize the "local count variable" based on the operator state
 *         for (Long l : countPerPartition.get()) {
 *             localCount += l;
 *         }
 *     }
 *
 *     public void snapshotState(FunctionSnapshotContext context) throws Exception {
 *         // the keyed state is always up to date anyways
 *         // just bring the per-partition state in shape
 *         countPerPartition.clear();
 *         countPerPartition.add(localCount);
 *     }
 *
 *     public T map(T value) throws Exception {
 *         // update the states
 *         countPerKey.add(1L);
 *         localCount++;
 *
 *         return value;
 *     }
 * }
 * }</pre>
 *
 * <hr>
 *
 * <h1><a name="shortcuts">Shortcuts</a></h1>
 * There are various ways that transformation functions can use state without implementing the
 * full-fledged {@code CheckpointedFunction} interface:
 *
 * <h4>Operator State</h4>
 * Checkpointing some state that is part of the function object itself is possible in a simpler way
 * by directly implementing the {@link ListCheckpointed} interface.
 * That mechanism is similar to the previously used {@link Checkpointed} interface.
 *
 * <h4>Keyed State</h4>
 * Access to keyed state is possible via the {@link RuntimeContext}'s methods:
 * <pre>{@code
 * public class CountPerKeyFunction<T> extends RichMapFunction<T, T> {
 *
 *     private ValueState<Long> count;
 *
 *     public void open(Configuration cfg) throws Exception {
 *         count = getRuntimeContext().getState(new ValueStateDescriptor<>("myCount", Long.class));
 *     }
 *
 *     public T map(T value) throws Exception {
 *         Long current = count.get();
 *         count.update(current == null ? 1L : current + 1);
 *
 *         return value;
 *     }
 * }
 * }</pre>
 *
 * @see ListCheckpointed
 * @see RuntimeContext
 */
@PublicEvolving
@SuppressWarnings("deprecation")
public interface CheckpointedFunction {

	/**
	 * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to the function to
	 * ensure that all state is exposed by means previously offered through {@link FunctionInitializationContext} when
	 * the Function was initialized, or offered now by {@link FunctionSnapshotContext} itself.
	 *
	 * @param context the context for drawing a snapshot of the operator
	 * @throws Exception
	 */
	void snapshotState(FunctionSnapshotContext context) throws Exception;

	/**
	 * This method is called when the parallel function instance is created during distributed
	 * execution. Functions typically set up their state storing data structures in this method.
	 *
	 * @param context the context for initializing the operator
	 * @throws Exception
	 */
	void initializeState(FunctionInitializationContext context) throws Exception;

}
