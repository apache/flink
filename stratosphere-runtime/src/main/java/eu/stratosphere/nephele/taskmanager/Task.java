/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ExecutionStateTransition;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Task implements ExecutionObserver {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(Task.class);

	private final ExecutionVertexID vertexID;

	private final RuntimeEnvironment environment;

	private final TaskManager taskManager;

	/**
	 * Stores whether the task has been canceled.
	 */
	private final AtomicBoolean canceled = new AtomicBoolean(false);

	/**
	 * The current execution state of the task
	 */
	private volatile ExecutionState executionState = ExecutionState.STARTING;

	
	private Queue<ExecutionListener> registeredListeners = new ConcurrentLinkedQueue<ExecutionListener>();

	public Task(ExecutionVertexID vertexID, final RuntimeEnvironment environment, TaskManager taskManager) {
		this.vertexID = vertexID;
		this.environment = environment;
		this.taskManager = taskManager;

		this.environment.setExecutionObserver(this);
	}


	/**
	 * Returns the ID of the job this task belongs to.
	 * 
	 * @return the ID of the job this task belongs to
	 */
	public JobID getJobID() {
		return this.environment.getJobID();
	}

	/**
	 * Returns the ID of this task.
	 * 
	 * @return the ID of this task
	 */
	public ExecutionVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * Returns the environment associated with this task.
	 * 
	 * @return the environment associated with this task
	 */
	public Environment getEnvironment() {
		return this.environment;
	}

	/**
	 * Marks the task as failed and triggers the appropriate state changes.
	 */
	public void markAsFailed() {
		executionStateChanged(ExecutionState.FAILED, "Execution thread died unexpectedly");
	}

	public void cancelExecution() {
		cancelOrKillExecution();
	}

	public void killExecution() {
		cancelOrKillExecution();
	}

	/**
	 * Cancels or kills the task.
	 *
	 * @param cancel <code>true/code> if the task shall be canceled, <code>false</code> if it shall be killed
	 */
	private void cancelOrKillExecution() {
		if (!this.canceled.compareAndSet(false, true)) {
			return;
		}

		if (this.executionState != ExecutionState.RUNNING && this.executionState != ExecutionState.FINISHING) {
			return;
		}

		executionStateChanged(ExecutionState.CANCELING, null);

		// Request user code to shut down
		try {
			this.environment.cancelExecution();
		} catch (Throwable e) {
			LOG.error("Error while cancelling the task.", e);
		}
	}

	/**
	 * Checks if the state of the thread which is associated with this task is <code>TERMINATED</code>.
	 * 
	 * @return <code>true</code> if the state of this thread which is associated with this task is
	 *         <code>TERMINATED</code>, <code>false</code> otherwise
	 */
	public boolean isTerminated() {
		final Thread executingThread = this.environment.getExecutingThread();
		if (executingThread.getState() == Thread.State.TERMINATED) {
			return true;
		}

		return false;
	}

	/**
	 * Starts the execution of this task.
	 */
	public void startExecution() {

		final Thread thread = this.environment.getExecutingThread();
		thread.start();
	}

	/**
	 * Registers the task manager profiler with the task.
	 * 
	 * @param taskManagerProfiler
	 *        the task manager profiler
	 * @param jobConfiguration
	 *        the configuration attached to the job
	 */
	public void registerProfiler(final TaskManagerProfiler taskManagerProfiler, final Configuration jobConfiguration) {
		taskManagerProfiler.registerExecutionListener(this, jobConfiguration);
	}

	/**
	 * Unregisters the task from the central memory manager.
	 * 
	 * @param memoryManager
	 *        the central memory manager
	 */
	public void unregisterMemoryManager(final MemoryManager memoryManager) {
		if (memoryManager != null) {
			memoryManager.releaseAll(this.environment.getInvokable());
		}
	}

	/**
	 * Unregisters the task from the task manager profiler.
	 * 
	 * @param taskManagerProfiler
	 *        the task manager profiler
	 */
	public void unregisterProfiler(final TaskManagerProfiler taskManagerProfiler) {
		if (taskManagerProfiler != null) {
			taskManagerProfiler.unregisterExecutionListener(this.vertexID);
		}
	}

	/**
	 * Returns the current execution state of the task.
	 * 
	 * @return the current execution state of the task
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	// -----------------------------------------------------------------------------------------------------------------
	//                                        ExecutionObserver methods
	// -----------------------------------------------------------------------------------------------------------------
	@Override
	public void executionStateChanged(final ExecutionState newExecutionState, final String optionalMessage) {

		// Check the state transition
		ExecutionStateTransition.checkTransition(false, getTaskName(), this.executionState, newExecutionState);

		// Make sure the reason for a transition to FAILED appears in the log files
		if (newExecutionState == ExecutionState.FAILED) {
			LOG.error(optionalMessage);
		}

		// Notify all listener objects
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.environment.getJobID(), this.vertexID, newExecutionState,
					optionalMessage);
		}

		// Store the new execution state
		this.executionState = newExecutionState;

		// Finally propagate the state change to the job manager
		this.taskManager.executionStateChanged(this.environment.getJobID(), this.vertexID, newExecutionState,
				optionalMessage);
	}

	/**
	 * Returns the name of the task associated with this observer object.
	 *
	 * @return the name of the task associated with this observer object
	 */
	private String getTaskName() {
		return this.environment.getTaskName() + " (" + (this.environment.getIndexInSubtaskGroup() + 1) + "/"
				+ this.environment.getCurrentNumberOfSubtasks() + ")";
	}


	@Override
	public void userThreadStarted(final Thread userThread) {
		// Notify the listeners
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().userThreadStarted(this.environment.getJobID(), this.vertexID, userThread);
		}
	}


	@Override
	public void userThreadFinished(final Thread userThread) {
		// Notify the listeners
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().userThreadFinished(this.environment.getJobID(), this.vertexID, userThread);
		}
	}

	/**
	 * Registers the {@link ExecutionListener} object for this task. This object
	 * will be notified about important events during the task execution.
	 *
	 * @param executionListener
	 *        the object to be notified for important events during the task execution
	 */

	public void registerExecutionListener(final ExecutionListener executionListener) {
		this.registeredListeners.add(executionListener);
	}

	/**
	 * Unregisters the {@link ExecutionListener} object for this environment. This object
	 * will no longer be notified about important events during the task execution.
	 *
	 * @param executionListener
	 *        the lister object to be unregistered
	 */

	public void unregisterExecutionListener(final ExecutionListener executionListener) {
		this.registeredListeners.remove(executionListener);
	}


	@Override
	public boolean isCanceled() {
		return this.canceled.get();
	}

	/**
	 * Returns the runtime environment associated with this task.
	 *
	 * @return the runtime environment associated with this task
	 */
	public RuntimeEnvironment getRuntimeEnvironment() {
		return this.environment;
	}
}
