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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.OperatorStateHandle;

import java.util.List;

/**
 * Interface that allows to implement different strategies for repartitioning of operator state as parallelism changes.
 */
public interface OperatorStateRepartitioner {

	/**
	 * @param previousParallelSubtaskStates List of state handles to the parallel subtask states of an operator, as they
	 *                                      have been checkpointed.
	 * @param newParallelism                The parallelism that we consider for the state redistribution. Determines the size of the
	 *                                      returned list.
	 * @return List with one entry per parallel subtask. Each subtask receives now one collection of states that build
	 * of the new total state for this subtask.
	 */
	List<List<OperatorStateHandle>> repartitionState(
			List<OperatorStateHandle> previousParallelSubtaskStates,
			int newParallelism);
}
