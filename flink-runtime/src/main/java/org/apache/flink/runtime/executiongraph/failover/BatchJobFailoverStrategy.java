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

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 *  Failover strategy for batch job scenario, that aware throwable type and apply different logic
 * */
public class BatchJobFailoverStrategy extends RestartPipelinedRegionStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(BatchJobFailoverStrategy.class);

	private int failLimit;

	public static final int MAX_ATTEMPTS_EXECUTION_FAILURE_COUNT = 6;

	/**
	 * Creates a new failover strategy for batch job scenario, that aware throwable type and apply different logic
	 *
	 * <p>The strategy will use the ExecutionGraph's future executor for callbacks.
	 *
	 * @param executionGraph The execution graph to handle.
	 */
	public BatchJobFailoverStrategy(ExecutionGraph executionGraph) {
		this(executionGraph, executionGraph.getFutureExecutor());
	}

	/**
	 * Creates a new failover strategy for batch job scenario, that aware throwable type and apply different logic
	 *
	 * @param executionGraph The execution graph to handle.
	 * @param callbackExecutor The executor that executes restart callbacks
	 */
	public BatchJobFailoverStrategy(ExecutionGraph executionGraph, Executor callbackExecutor) {
		super(executionGraph, callbackExecutor, true);

		if(!(this.executionGraph.getRestartStrategy() instanceof NoRestartStrategy)){
			throw new FlinkRuntimeException("BatchJobFailoverStrategy can only work with NoRestartStrategy");
		}

		//TODO: use JobManagerOptions.MAX_ATTEMPTS_EXECUTION_FAILURE_COUNT when this feature complete
		this.failLimit = MAX_ATTEMPTS_EXECUTION_FAILURE_COUNT;
		// currently we use a hardcode value as we don't want to expose this configuration to public document to before we release it.
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {

		ThrowableType type = ThrowableClassifier.getThrowableType(cause);

		//should fail the job, as there is a non recoverable failure happens
		if(type == ThrowableType.NonRecoverableError){
			LOG.info("Task {} (#{}) cannot recover from this failure, will fail the job: {}",
				taskExecution.getVertex().getTaskNameWithSubtaskIndex(), taskExecution.getAttemptNumber(), cause.toString());
			executionGraph.failGlobal(cause);
			return;
		}

		// TODO:if its a PartitionMissingError, apply revocation logic
		// TODO: if its a EnvironmentError}, apply blackList logic
		final ExecutionVertex ev = taskExecution.getVertex();
		final FailoverRegion failoverRegion = vertexToRegion.get(ev);

		if (failoverRegion == null) {
			executionGraph.failGlobal(new FlinkException(
				"Can not find a failover region for the execution " + ev.getTaskNameWithSubtaskIndex(), cause));
		}
		else {
			// Other failures are recoverable, lets try to restart the task
			// Before restart, lets check if the failure exceed the limit
			if(failoverRegion.getFailCount() >= this.failLimit){
				executionGraph.failGlobal(new FlinkException(
					"Max fail recovering attempt achieved, region failed " + this.failLimit +" times", cause));
				return;
			}

			LOG.info("Recovering task failure for {} #{} ({}) via restart of failover region",
				taskExecution.getVertex().getTaskNameWithSubtaskIndex(),
				taskExecution.getAttemptNumber(),
				taskExecution.getAttemptId());

			failoverRegion.onExecutionFail(taskExecution, cause);
		}
	}

	@Override
	public String getStrategyName() {
		return "Batch Job Failover Strategy";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the BatchJobFailoverStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public BatchJobFailoverStrategy create(ExecutionGraph executionGraph) {
			return new BatchJobFailoverStrategy(executionGraph);
		}
	}
}
