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
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple failover strategy that restarts each task individually.
 * This strategy is only applicable if the entire job consists unconnected
 * tasks, meaning each task is its own component.
 */
public class RestartIndividualStrategy extends FailoverStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(RestartIndividualStrategy.class);

	// ------------------------------------------------------------------------

	/** The execution graph to recover */
	private final ExecutionGraph executionGraph;

	/** The executor that executes restart callbacks */
	private final Executor callbackExecutor;

	private final SimpleCounter numTaskFailures;

	/**
	 * Creates a new failover strategy that recovers from failures by restarting only the failed task
	 * of the execution graph.
	 * 
	 * <p>The strategy will use the ExecutionGraph's future executor for callbacks.
	 * 
	 * @param executionGraph The execution graph to handle.
	 */
	public RestartIndividualStrategy(ExecutionGraph executionGraph) {
		this(executionGraph, executionGraph.getFutureExecutor());
	}

	/**
	 * Creates a new failover strategy that recovers from failures by restarting only the failed task
	 * of the execution graph.
	 *
	 * @param executionGraph The execution graph to handle.
	 * @param callbackExecutor The executor that executes restart callbacks
	 */
	public RestartIndividualStrategy(ExecutionGraph executionGraph, Executor callbackExecutor) {
		this.executionGraph = checkNotNull(executionGraph);
		this.callbackExecutor = checkNotNull(callbackExecutor);

		this.numTaskFailures = new SimpleCounter();
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {

		// to better handle the lack of resources (potentially by a scale-in), we
		// make failures due to missing resources global failures 
		if (cause instanceof NoResourceAvailableException) {
			LOG.info("Not enough resources to schedule {} - triggering full recovery.", taskExecution);
			executionGraph.failGlobal(cause);
			return;
		}

		LOG.info("Recovering task failure for {} (#{}) via individual restart.", 
				taskExecution.getVertex().getTaskNameWithSubtaskIndex(), taskExecution.getAttemptNumber());

		numTaskFailures.inc();

		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing
		final CompletableFuture<ExecutionState> terminationFuture = taskExecution.getTerminalStateFuture();

		final ExecutionVertex vertexToRecover = taskExecution.getVertex(); 
		final long globalModVersion = taskExecution.getGlobalModVersion();

		terminationFuture.thenAcceptAsync(
			(ExecutionState value) -> {
				try {
					long createTimestamp = System.currentTimeMillis();
					Execution newExecution = vertexToRecover.resetForNewExecution(createTimestamp, globalModVersion);
					newExecution.scheduleForExecution();
				}
				catch (GlobalModVersionMismatch e) {
					// this happens if a concurrent global recovery happens. simply do nothing.
				}
				catch (Exception e) {
					executionGraph.failGlobal(
							new Exception("Error during fine grained recovery - triggering full recovery", e));
				}
			},
			callbackExecutor);
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// we validate here that the vertices are in fact not connected to
		// any other vertices
		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			List<IntermediateResult> inputs = ejv.getInputs();
			IntermediateResult[] outputs = ejv.getProducedDataSets();

			if ((inputs != null && inputs.size() > 0) || (outputs != null && outputs.length > 0)) {
				throw new FlinkRuntimeException("Incompatible failover strategy - strategy '" + 
						getStrategyName() + "' can only handle jobs with only disconnected tasks.");
			}
		}
	}

	@Override
	public String getStrategyName() {
		return "Individual Task Restart";
	}

	@Override
	public void registerMetrics(MetricGroup metricGroup) {
		metricGroup.counter("task_failures", numTaskFailures);
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartAllStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public RestartIndividualStrategy create(ExecutionGraph executionGraph) {
			return new RestartIndividualStrategy(executionGraph);
		}
	}
}
