/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionNotifiable;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

public class EnvironmentListenerImpl implements ExecutionNotifiable {

	private static final Log LOG = LogFactory.getLog(EnvironmentListenerImpl.class);

	private final TaskManagerProfilerImpl taskManagerProfiler;

	private final ExecutionVertexID executionVertexID;

	public EnvironmentListenerImpl(TaskManagerProfilerImpl taskManagerProfiler, ExecutionVertexID id,
			Environment environment, long timerInterval) {

		this.taskManagerProfiler = taskManagerProfiler;
		this.executionVertexID = id;
	}

	/*
	 * public void reportCPUUtilization(long timestamp, long totalCPUTime, long totalCPUUserTime, long totalCPUWaitTime,
	 * long totalCPUBlockTime) {
	 * //Check if the received values are valid
	 * if(totalCPUTime < 0 || totalCPUUserTime < 0 || totalCPUWaitTime < 0 || totalCPUBlockTime < 0) {
	 * return;
	 * }
	 * //Calculate the length of the measured interval
	 * final long interval = (timestamp - this.lastTimestamp);
	 * final long cputime = totalCPUTime - this.lastTotalCPUTime;
	 * final long usrtime = totalCPUUserTime - this.lastTotalCPUUserTime;
	 * final long systime = cputime - usrtime;
	 * final long waitime = totalCPUWaitTime - this.lastTotalCPUWaitTime;
	 * final long blktime = totalCPUBlockTime - this.lastTotalCPUBlockTime;
	 * final InternalExecutionVertexThreadProfilingData threadData = new InternalExecutionVertexThreadProfilingData(
	 * this.environment.getJobID(),
	 * this.executionVertexID,
	 * (int) interval,
	 * (int) ((usrtime*PERCENT)/interval),
	 * (int) ((systime*PERCENT)/interval),
	 * (int) ((blktime*PERCENT)/interval),
	 * (int) ((waitime*PERCENT)/interval));
	 * //Publish thread profiling data
	 * this.taskManagerProfiler.publishProfilingData(threadData);
	 * //Finally, prepare for next call of this method
	 * this.lastTimestamp = timestamp;
	 * this.lastTotalCPUTime = totalCPUTime;
	 * this.lastTotalCPUUserTime = totalCPUUserTime;
	 * this.lastTotalCPUWaitTime = totalCPUWaitTime;
	 * this.lastTotalCPUBlockTime = totalCPUBlockTime;
	 * }
	 */

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(Environment ee, ExecutionState newExecutionState, String optionalMessage) {

		switch (newExecutionState) {
		case RUNNING:
			this.taskManagerProfiler.registerMainThreadForCPUProfiling(ee, ee.getExecutingThread(),
				this.executionVertexID);
			break;
		case FINISHING:
		case FINISHED:
		case CANCELLED:
		case FAILED:
			this.taskManagerProfiler.unregisterMainThreadFromCPUProfiling(ee, ee.getExecutingThread());
			break;
		default:
			LOG.error("Unexpected state transition to " + " for vertex " + this.executionVertexID);
			break;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(Environment ee, Thread userThread) {

		// Make sure the user thread is not the task's main thread
		if (ee.getExecutingThread() == userThread) {
			return;
		}

		this.taskManagerProfiler.unregisterUserThreadFromCPUProfiling(ee, userThread);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(Environment ee, Thread userThread) {

		// Make sure the user thread is not the task's main thread
		if (ee.getExecutingThread() == userThread) {
			return;
		}

		this.taskManagerProfiler.registerUserThreadForCPUProfiling(ee, userThread);
	}
}
