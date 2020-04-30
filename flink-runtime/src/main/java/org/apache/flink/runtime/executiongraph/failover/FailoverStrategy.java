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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import java.util.List;

/**
 * A {@code FailoverStrategy} describes how the job computation recovers from task
 * failures.
 * 
 * <p>Failover strategies implement recovery logic for failures of tasks. The execution
 * graph still implements "global failure / recovery" (which restarts all tasks) as
 * a fallback plan or safety net in cases where it deems that the state of the graph
 * may have become inconsistent.
 */
public abstract class FailoverStrategy {


	// ------------------------------------------------------------------------
	//  failover implementation
	// ------------------------------------------------------------------------ 

	/**
	 * Called by the execution graph when a task failure occurs.
	 * 
	 * @param taskExecution The execution attempt of the failed task. 
	 * @param cause The exception that caused the task failure.
	 */
	public abstract void onTaskFailure(Execution taskExecution, Throwable cause);

	/**
	 * Called whenever new vertices are added to the ExecutionGraph.
	 * 
	 * @param newJobVerticesTopological The newly added vertices, in topological order.
	 */
	public abstract void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological);

	/**
	 * Gets the name of the failover strategy, for logging purposes.
	 */
	public abstract String getStrategyName();

	/**
	 * Tells the FailoverStrategy to register its metrics.
	 * 
	 * <p>The default implementation does nothing
	 * 
	 * @param metricGroup The metric group to register the metrics at
	 */
	public void registerMetrics(MetricGroup metricGroup) {}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * This factory is a necessary indirection when creating the FailoverStrategy to that
	 * we can have both the FailoverStrategy final in the ExecutionGraph, and the
	 * ExecutionGraph final in the FailOverStrategy.
	 */
	public interface Factory {

		/**
		 * Instantiates the {@code FailoverStrategy}.
		 * 
		 * @param executionGraph The execution graph for which the strategy implements failover.
		 * @return The instantiated failover strategy.
		 */
		FailoverStrategy create(ExecutionGraph executionGraph);
	}
}
