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

package org.apache.flink.runtime.taskmanager;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionListener;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.profiling.TaskManagerProfiler;
import org.apache.flink.util.ExceptionUtils;

public final class Task {

	/** For atomic state updates */
	private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER = 
			AtomicReferenceFieldUpdater.newUpdater(Task.class, ExecutionState.class, "executionState");
			
	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(Task.class);

	// --------------------------------------------------------------------------------------------
	
	private final JobID jobId;
	
	private final JobVertexID vertexId;
	
	private final int subtaskIndex;
	
	private final int numberOfSubtasks;
	
	private final ExecutionAttemptID executionId;
	
	private final String taskName;

	private final TaskManager taskManager;
	
	
	private final List<ExecutionListener> executionListeners = new CopyOnWriteArrayList<ExecutionListener>();

	/** The environment (with the invokable) executed by this task */
	private volatile RuntimeEnvironment environment;
	
	/** The current execution state of the task */
	private volatile ExecutionState executionState = ExecutionState.DEPLOYING;

	// --------------------------------------------------------------------------------------------	
	
	public Task(JobID jobId, JobVertexID vertexId, int taskIndex, int parallelism, 
			ExecutionAttemptID executionId, String taskName, TaskManager taskManager)
	{
		this.jobId = jobId;
		this.vertexId = vertexId;
		this.subtaskIndex = taskIndex;
		this.numberOfSubtasks = parallelism;
		this.executionId = executionId;
		this.taskName = taskName;
		this.taskManager = taskManager;
	}


	/**
	 * Returns the ID of the job this task belongs to.
	 * 
	 * @return the ID of the job this task belongs to
	 */
	public JobID getJobID() {
		return this.jobId;
	}

	/**
	 * Returns the ID of this task vertex.
	 * 
	 * @return the ID of this task vertex.
	 */
	public JobVertexID getVertexID() {
		return this.vertexId;
	}

	/**
	 * Gets the index of the parallel subtask [0, parallelism).
	 * 
	 * @return The task index of the parallel subtask.
	 */
	public int getSubtaskIndex() {
		return subtaskIndex;
	}
	
	/**
	 * Gets the total number of subtasks of the task that this subtask belongs to.
	 * 
	 * @return The total number of this task's subtasks.
	 */
	public int getNumberOfSubtasks() {
		return numberOfSubtasks;
	}
	
	/**
	 * Gets the ID of the execution attempt.
	 * 
	 * @return The ID of the execution attempt.
	 */
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}
	
	/**
	 * Returns the current execution state of the task.
	 * 
	 * @return the current execution state of the task
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}
	
	public void setEnvironment(RuntimeEnvironment environment) {
		this.environment = environment;
	}
	
	public RuntimeEnvironment getEnvironment() {
		return environment;
	}
	
	public boolean isCanceledOrFailed() {
		return executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED ||
				executionState == ExecutionState.FAILED;
	}
	
	public String getTaskName() {
		return taskName;
	}
	
	public String getTaskNameWithSubtasks() {
		return this.taskName + " (" + (this.subtaskIndex + 1) + "/" + this.numberOfSubtasks + ")";
	}
	
	// ----------------------------------------------------------------------------------------------------------------
	//  States and Transitions
	// ----------------------------------------------------------------------------------------------------------------
	
	/**
	 * Marks the task as finished. This succeeds, if the task was previously in the state
	 * "RUNNING", otherwise it fails. Failure indicates that the task was either
	 * canceled, or set to failed.
	 * 
	 * @return True, if the task correctly enters the state FINISHED.
	 */
	public boolean markAsFinished() {
		if (STATE_UPDATER.compareAndSet(this, ExecutionState.RUNNING, ExecutionState.FINISHED)) {
			notifyObservers(ExecutionState.FINISHED, null);
			taskManager.notifyExecutionStateChange(jobId, executionId, ExecutionState.FINISHED, null);
			return true;
		} else {
			return false;
		}
	}
	
	public void markFailed(Throwable error) {
		while (true) {
			ExecutionState current = this.executionState;
			
			// if canceled, fine. we are done, and the jobmanager has been told
			if (current == ExecutionState.CANCELED) {
				return;
			}
			
			// if canceling, we are done, but we cannot be sure that the jobmanager has been told.
			// after all, we may have recognized our failure state before the cancelling and never sent a canceled
			// message back
			else if (STATE_UPDATER.compareAndSet(this, current, ExecutionState.FAILED)) {
				notifyObservers(ExecutionState.FAILED, ExceptionUtils.stringifyException(error));
				taskManager.notifyExecutionStateChange(jobId, executionId, ExecutionState.FAILED, error);
				return;
			}
		}
	}
	
	public void cancelExecution() {
		while (true) {
			ExecutionState current = this.executionState;
			
			// if the task is already canceled (or canceling) or finished or failed,
			// then we need not do anything
			if (current == ExecutionState.FINISHED || current == ExecutionState.CANCELED ||
					current == ExecutionState.CANCELING || current == ExecutionState.FAILED)
			{
				return;
			}
			
			if (current == ExecutionState.DEPLOYING) {
				// directly set to canceled
				if (STATE_UPDATER.compareAndSet(this, current, ExecutionState.CANCELED)) {
					
					notifyObservers(ExecutionState.CANCELED, null);
					taskManager.notifyExecutionStateChange(jobId, executionId, ExecutionState.CANCELED, null);
					return;
				}
			}
			else if (current == ExecutionState.RUNNING) {
				// go to canceling and perform the actual task canceling
				if (STATE_UPDATER.compareAndSet(this, current, ExecutionState.CANCELING)) {
					
					notifyObservers(ExecutionState.CANCELING, null);
					try {
						this.environment.cancelExecution();
					} catch (Throwable e) {
						LOG.error("Error while cancelling the task.", e);
					}
					
					return;
				}
			}
			else {
				throw new RuntimeException("unexpected state for cancelling: " + current);
			}
		}
	}
	
	/**
	 * Sets the tasks to be cancelled and reports a failure back to the master.
	 * This method is important if a failure needs to be reported to the master, because
	 * a simple canceled m
	 * 
	 * @param cause The exception to report in the error message
	 */
	public void failExternally(Throwable cause) {
		while (true) {
			ExecutionState current = this.executionState;
			
			// if the task is already canceled (or canceling) or finished or failed,
			// then we need not do anything
			if (current == ExecutionState.FINISHED || current == ExecutionState.CANCELED ||
					current == ExecutionState.CANCELING || current == ExecutionState.FAILED)
			{
				return;
			}
			
			if (current == ExecutionState.DEPLOYING) {
				// directly set to canceled
				if (STATE_UPDATER.compareAndSet(this, current, ExecutionState.FAILED)) {
					
					notifyObservers(ExecutionState.FAILED, null);
					taskManager.notifyExecutionStateChange(jobId, executionId, ExecutionState.FAILED, cause);
					return;
				}
			}
			else if (current == ExecutionState.RUNNING) {
				// go to canceling and perform the actual task canceling
				if (STATE_UPDATER.compareAndSet(this, current, ExecutionState.FAILED)) {
					try {
						this.environment.cancelExecution();
					} catch (Throwable e) {
						LOG.error("Error while cancelling the task.", e);
					}
					
					notifyObservers(ExecutionState.FAILED, null);
					taskManager.notifyExecutionStateChange(jobId, executionId, ExecutionState.FAILED, cause);
					
					return;
				}
			}
			else {
				throw new RuntimeException("unexpected state for cancelling: " + current);
			}
		}
	}
	
	public void cancelingDone() {
		while (true) {
			ExecutionState current = this.executionState;
			
			if (current == ExecutionState.CANCELED || current == ExecutionState.FAILED) {
				return;
			}
			if (!(current == ExecutionState.RUNNING || current == ExecutionState.CANCELING)) {
				LOG.error(String.format("Unexpected state transition in Task: %s -> %s", current, ExecutionState.CANCELED));
			}
			
			if (STATE_UPDATER.compareAndSet(this, current, ExecutionState.CANCELED)) {
				notifyObservers(ExecutionState.CANCELED, null);
				taskManager.notifyExecutionStateChange(jobId, executionId, ExecutionState.CANCELED, null);
				return;
			}
		}
	}

	/**
	 * Starts the execution of this task.
	 */
	public boolean startExecution() {
		if (STATE_UPDATER.compareAndSet(this, ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
			final Thread thread = this.environment.getExecutingThread();
			thread.start();
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Unregisters the task from the central memory manager.
	 * 
	 * @param memoryManager
	 *        the central memory manager
	 */
	public void unregisterMemoryManager(MemoryManager memoryManager) {
		RuntimeEnvironment env = this.environment;
		if (memoryManager != null && env != null) {
			memoryManager.releaseAll(env.getInvokable());
		}
	}
	
	// -----------------------------------------------------------------------------------------------------------------
	//                                        Task Profiling
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Registers the task manager profiler with the task.
	 * 
	 * @param taskManagerProfiler
	 *        the task manager profiler
	 * @param jobConfiguration
	 *        the configuration attached to the job
	 */
	public void registerProfiler(TaskManagerProfiler taskManagerProfiler, Configuration jobConfiguration) {
		taskManagerProfiler.registerExecutionListener(this, jobConfiguration);
	}

	/**
	 * Unregisters the task from the task manager profiler.
	 * 
	 * @param taskManagerProfiler
	 *        the task manager profiler
	 */
	public void unregisterProfiler(TaskManagerProfiler taskManagerProfiler) {
		if (taskManagerProfiler != null) {
			taskManagerProfiler.unregisterExecutionListener(this.executionId);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     State Listeners
	// --------------------------------------------------------------------------------------------
	
	public void registerExecutionListener(ExecutionListener listener) {
		if (listener == null) {
			throw new IllegalArgumentException();
		}
		this.executionListeners.add(listener);
	}

	public void unregisterExecutionListener(ExecutionListener listener) {
		if (listener == null) {
			throw new IllegalArgumentException();
		}
		this.executionListeners.remove(listener);
	}
	
	private void notifyObservers(ExecutionState newState, String message) {
		for (ExecutionListener listener : this.executionListeners) {
			try {
				listener.executionStateChanged(jobId, vertexId, subtaskIndex, executionId, newState, message);
			}
			catch (Throwable t) {
				LOG.error("Error while calling execution listener.", t);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                       Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return getTaskNameWithSubtasks() + " [" + executionState + ']';
	}
}
