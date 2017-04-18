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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that restarts regions of the ExecutionGraph. A region is defined
 * by this strategy as the weakly connected component of tasks that communicate via pipelined
 * data exchange.
 */
public class RestartPipelinedRegionStrategy extends FailoverStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(RestartPipelinedRegionStrategy.class);

	/** The execution graph on which this FailoverStrategy works */
	private final ExecutionGraph executionGraph;

	/** The executor used for future actions */
	private final Executor executor;

	/** Fast lookup from vertex to failover region */
	private final HashMap<ExecutionVertex, FailoverRegion> vertexToRegion;


	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given
	 * execution graph and uses the execution graph's future executor to call restart actions.
	 * 
	 * @param executionGraph The execution graph on which this FailoverStrategy will work
	 */
	public RestartPipelinedRegionStrategy(ExecutionGraph executionGraph) {
		this(executionGraph, executionGraph.getFutureExecutor());
	}

	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given
	 * execution graph and uses the given executor to call restart actions.
	 * 
	 * @param executionGraph The execution graph on which this FailoverStrategy will work
	 * @param executor  The executor used for future actions
	 */
	public RestartPipelinedRegionStrategy(ExecutionGraph executionGraph, Executor executor) {
		this.executionGraph = checkNotNull(executionGraph);
		this.executor = checkNotNull(executor);
		this.vertexToRegion = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  failover implementation
	// ------------------------------------------------------------------------ 

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		final ExecutionVertex ev = taskExecution.getVertex();
		final FailoverRegion failoverRegion = vertexToRegion.get(ev);

		if (failoverRegion == null) {
			executionGraph.failGlobal(new FlinkException(
					"Can not find a failover region for the execution " + ev.getTaskNameWithSubtaskIndex()));
		}
		else {
			failoverRegion.onExecutionFail(taskExecution, cause);
		}
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		LOG.debug("Generating failover regions for {} new job vertices", newJobVerticesTopological.size());
		generateAllFailoverRegion(newJobVerticesTopological);
	}

	@Override
	public String getStrategyName() {
		return "Pipelined Region Failover";
	}

	// Generate all the FailoverRegion from the new added job vertexes
	private void generateAllFailoverRegion(List<ExecutionJobVertex> newJobVerticesTopological) {
		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				if (getFailoverRegion(ev) != null) {
					continue;
				}
				List<ExecutionVertex> pipelinedExecutions = new ArrayList<>();
				List<ExecutionVertex> orgExecutions = new ArrayList<>();
				orgExecutions.add(ev);
				pipelinedExecutions.add(ev);
				getAllPipelinedConnectedVertexes(orgExecutions, pipelinedExecutions);

				FailoverRegion region = new FailoverRegion(executionGraph, executor, pipelinedExecutions);
				for (ExecutionVertex vertex : pipelinedExecutions) {
					vertexToRegion.put(vertex, region);
				}
			}
		}
	}

	/**
	 * Get all connected executions of the original executions
	 *
	 * @param orgExecutions  the original execution vertexes
	 * @param connectedExecutions  the total connected executions
	 */
	private static void getAllPipelinedConnectedVertexes(List<ExecutionVertex> orgExecutions, List<ExecutionVertex> connectedExecutions) {
		List<ExecutionVertex> newAddedExecutions = new ArrayList<>();
		for (ExecutionVertex ev : orgExecutions) {
			// Add downstream ExecutionVertex
			for (IntermediateResultPartition irp : ev.getProducedPartitions().values()) {
				if (irp.getIntermediateResult().getResultType().isPipelined()) {
					for (List<ExecutionEdge> consumers : irp.getConsumers()) {
						for (ExecutionEdge consumer : consumers) {
							ExecutionVertex cev = consumer.getTarget();
							if (!connectedExecutions.contains(cev)) {
								newAddedExecutions.add(cev);
							}
						}
					}
				}
			}
			if (!newAddedExecutions.isEmpty()) {
				connectedExecutions.addAll(newAddedExecutions);
				getAllPipelinedConnectedVertexes(newAddedExecutions, connectedExecutions);
				newAddedExecutions.clear();
			}
			// Add upstream ExecutionVertex
			int inputNum = ev.getNumberOfInputs();
			for (int i = 0; i < inputNum; i++) {
				for (ExecutionEdge input : ev.getInputEdges(i)) {
					if (input.getSource().getIntermediateResult().getResultType().isPipelined()) {
						ExecutionVertex pev = input.getSource().getProducer();
						if (!connectedExecutions.contains(pev)) {
							newAddedExecutions.add(pev);
						}
					}
				}
			}
			if (!newAddedExecutions.isEmpty()) {
				connectedExecutions.addAll(0, newAddedExecutions);
				getAllPipelinedConnectedVertexes(newAddedExecutions, connectedExecutions);
				newAddedExecutions.clear();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  testing
	// ------------------------------------------------------------------------

	/**
	 * Finds the failover region that contains the given execution vertex.
 	 */
	@VisibleForTesting
	public FailoverRegion getFailoverRegion(ExecutionVertex ev) {
		return vertexToRegion.get(ev);
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the RestartPipelinedRegionStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartPipelinedRegionStrategy(executionGraph);
		}
	}
}
