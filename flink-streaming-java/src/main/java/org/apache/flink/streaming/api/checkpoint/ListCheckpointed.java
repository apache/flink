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

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.List;

/**
 * This interface can be implemented by functions that want to store state in checkpoints.
 * It can be used in a similar way as the deprecated {@link Checkpointed} interface, but
 * supports <b>list-style state redistribution</b> for cases when the parallelism of the
 * transformation is changed.
 *
 * <p>Implementing this interface is a shortcut for obtaining the default {@code ListState}
 * from the {@link OperatorStateStore}. Using the {@code OperatorStateStore} directly gives
 * more flexible options to use operator state, for example controlling the serialization
 * of the state objects, or have multiple named states.
 *
 * <h2>State Redistribution</h2>
 * State redistribution happens when the parallelism of the operator is changed.
 * State redistribution of <i>operator state</i> (to which category the state handled by this
 * interface belongs) always goes through a checkpoint, so it appears
 * to the transformation functions like a failure/recovery combination, where recovery happens
 * with a different parallelism.
 *
 * <p>Conceptually, the state in the checkpoint is the concatenated list of all lists
 * returned by the parallel transformation function instances. When restoring from a checkpoint,
 * the list is divided into sub-lists that are assigned to each parallel function instance.
 *
 * <p>The following sketch illustrates the state redistribution.The function runs with parallelism
 * <i>3</i>. The first two parallel instance of the function return lists with two state elements,
 * the third one a list with one element.
 * <pre>
 *    func_1        func_2     func_3
 * +----+----+   +----+----+   +----+
 * | S1 | S2 |   | S3 | S4 |   | S5 |
 * +----+----+   +----+----+   +----+
 * </pre>
 *
 * <p>Recovering the checkpoint with <i>parallelism = 5</i> yields the following state assignment:
 * <pre>
 * func_1   func_2   func_3   func_4   func_5
 * +----+   +----+   +----+   +----+   +----+
 * | S1 |   | S2 |   | S3 |   | S4 |   | S5 |
 * +----+   +----+   +----+   +----+   +----+
 * </pre>

 * Recovering the checkpoint with <i>parallelism = 2</i> yields the following state assignment:
 * <pre>
 *      func_1          func_2
 * +----+----+----+   +----+----+
 * | S1 | S2 | S3 |   | S4 | S5 |
 * +----+----+----+   +----+----+
 * </pre>
 *
 * <h2>Example</h2>
 * The following example illustrates how to implement a {@code MapFunction} that counts all elements
 * passing through it, keeping the total count accurate under re-scaling  (changes or parallelism):
 * <pre>{@code
 * public class CountingFunction<T> implements MapFunction<T, Tuple2<T, Long>>, ListCheckpointed<Long> {
 *
 *     // this count is the number of elements in the parallel subtask
 *     private long count;
 *
 *     {@literal @}Override
 *     public List<Long> snapshotState(long checkpointId, long timestamp) {
 *         // return a single element - our count
 *         return Collections.singletonList(count);
 *     }
 *
 *     {@literal @}Override
 *     public void restoreState(List<Long> state) throws Exception {
 *         // in case of scale in, this adds up counters from different original subtasks
 *         // in case of scale out, list this may be empty
 *         for (Long l : state) {
 *             count += l;
 *         }
 *     }
 *
 *     {@literal @}Override
 *     public Tuple2<T, Long> map(T value) {
 *         count++;
 *         return new Tuple2<>(value, count);
 *     }
 * }
 * }</pre>
 *
 * @param <T> The type of the operator state.
 */
@PublicEvolving
public interface ListCheckpointed<T extends Serializable> {

	/**
	 * Gets the current state of the function. The state must reflect the result of all prior
	 * invocations to this function.
	 *
	 * <p>The returned list should contain one entry for redistributable unit of state. See
	 * the {@link ListCheckpointed class docs} for an illustration how list-style state
	 * redistribution works.
	 *
	 * <p>As special case, the returned list may be null or empty (if the operator has no state)
	 * or it may contain a single element (if the operator state is indivisible).
	 *
	 * @param checkpointId The ID of the checkpoint - a unique and monotonously increasing value.
	 * @param timestamp The wall clock timestamp when the checkpoint was triggered by the master.
	 *
	 * @return The operator state in a list of redistributable, atomic sub-states.
	 *         Should not return null, but empty list instead.
	 *
	 * @throws Exception Thrown if the creation of the state object failed. This causes the
	 *                   checkpoint to fail. The system may decide to fail the operation (and trigger
	 *                   recovery), or to discard this checkpoint attempt and to continue running
	 *                   and to try again with the next checkpoint attempt.
	 */
	List<T> snapshotState(long checkpointId, long timestamp) throws Exception;

	/**
	 * Restores the state of the function or operator to that of a previous checkpoint.
	 * This method is invoked when the function is executed after a failure recovery.
	 * The state list may be empty if no state is to be recovered by the particular parallel instance
	 * of the function.
	 *
	 * <p>The given state list will contain all the <i>sub states</i> that this parallel
	 * instance of the function needs to handle. Refer to the  {@link ListCheckpointed class docs}
	 * for an illustration how list-style state redistribution works.
	 *
	 * <p><b>Important:</b> When implementing this interface together with {@link RichFunction},
	 * then the {@code restoreState()} method is called before {@link RichFunction#open(Configuration)}.
	 *
	 * @param state The state to be restored as a list of atomic sub-states.
	 *
	 * @throws Exception Throwing an exception in this method causes the recovery to fail.
	 *                   The exact consequence depends on the configured failure handling strategy,
	 *                   but typically the system will re-attempt the recovery, or try recovering
	 *                   from a different checkpoint.
	 */
	void restoreState(List<T> state) throws Exception;
}
