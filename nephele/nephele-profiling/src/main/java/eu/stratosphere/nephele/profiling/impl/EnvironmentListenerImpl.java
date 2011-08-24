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
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

public class EnvironmentListenerImpl implements ExecutionListener {

	private static final Log LOG = LogFactory.getLog(EnvironmentListenerImpl.class);

	private final TaskManagerProfilerImpl taskManagerProfiler;

	private final ExecutionVertexID executionVertexID;

	public EnvironmentListenerImpl(TaskManagerProfilerImpl taskManagerProfiler, ExecutionVertexID id,
			Environment environment, long timerInterval) {

		this.taskManagerProfiler = taskManagerProfiler;
		this.executionVertexID = id;
	}

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
		case CANCELING:
		case CANCELED:
		case FAILED:
			this.taskManagerProfiler.unregisterMainThreadFromCPUProfiling(ee, ee.getExecutingThread());
			break;
		default:
			LOG.error("Unexpected state transition to " + newExecutionState + " for vertex " + this.executionVertexID);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialExecutionResourcesExhausted(Environment ee) {
		// TODO Auto-generated method stub
		
	}
}
