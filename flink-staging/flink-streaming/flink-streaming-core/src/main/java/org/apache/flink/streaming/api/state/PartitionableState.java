/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.state;

import org.apache.flink.runtime.state.OperatorState;

/**
 * Base class for representing operator states that can be repartitioned for
 * state state and load balancing.
 * 
 * @param <T>
 *            The type of the operator state.
 */
public abstract class PartitionableState<T> extends OperatorState<T> {

	private static final long serialVersionUID = 1L;

	PartitionableState(T initialState) {
		super(initialState);
	}

	/**
	 * Repartitions(divides) the current state into the given number of new
	 * partitions. The created partitions will be used to redistribute then
	 * rebuild the state among the parallel instances of the operator. The
	 * implementation should reflect the partitioning of the input values to
	 * maintain correct operator behavior.
	 * 
	 * </br> </br> It is also assumed that if we would {@link #reBuild} the
	 * repartitioned state we would basically get the same as before.
	 * 
	 * 
	 * @param numberOfPartitions
	 *            The desired number of partitions. The method must return an
	 *            array of that size.
	 * @return The array containing the state part for each partition.
	 */
	public abstract OperatorState<T>[] repartition(int numberOfPartitions);

	/**
	 * Rebuilds the current state partition from the given parts. Used for
	 * building the state after a re-balance phase.
	 * 
	 * @param parts
	 *            The state parts that will be used to rebuild the current
	 *            partition.
	 * @return The rebuilt operator state.
	 */
	public abstract OperatorState<T> reBuild(OperatorState<T>... parts);

}
