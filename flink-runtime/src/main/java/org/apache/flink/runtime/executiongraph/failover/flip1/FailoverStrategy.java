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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Set;

/**
 * New interface for failover strategies.
 */
public interface FailoverStrategy {

	/**
	 * Returns a set of IDs corresponding to the set of vertices that should be restarted.
	 *
	 * @param executionVertexId ID of the failed task
	 * @param cause cause of the failure
	 * @return set of IDs of vertices to restart
	 */
	Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause);

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * The factory to instantiate {@link FailoverStrategy}.
	 */
	interface Factory {

		/**
		 * Instantiates the {@link FailoverStrategy}.
		 *
		 * @param topology of the graph to failover
		 * @param resultPartitionAvailabilityChecker to check whether a result partition is available
		 * @return The instantiated failover strategy.
		 */
		FailoverStrategy create(
			FailoverTopology<?, ?> topology,
			ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker);
	}
}
