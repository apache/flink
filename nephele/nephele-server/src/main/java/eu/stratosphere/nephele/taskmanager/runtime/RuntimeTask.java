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

package eu.stratosphere.nephele.taskmanager.runtime;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.CheckpointDecisionRequester;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ExecutionStateTransition;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.util.StringUtils;

public final class RuntimeTask implements Task, ExecutionObserver {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(RuntimeTask.class);

	private final ExecutionVertexID vertexID;

	private final RuntimeEnvironment environment;

	private final CheckpointState initialCheckpointState;

	private final TaskManager taskManager;

	/**
	 * Stores whether the task has been canceled.
	 */
	private volatile boolean isCanceled = false;

	/**
	 * The current execution state of the task
	 */
	private volatile ExecutionState executionState = ExecutionState.STARTING;

	/**
	 * If the task creates a checkpoint at runtime, a checkpoint decision can be asynchronously requested through this
	 * interface.
	 */
	private volatile CheckpointDecisionRequester checkpointDecisionRequester = null;

	private Queue<ExecutionListener> registeredListeners = new ConcurrentLinkedQueue<ExecutionListener>();

	public RuntimeTask(final ExecutionVertexID vertexID, final RuntimeEnvironment environment,
			final CheckpointState initialCheckpointState, final TaskManager taskManager) {

		this.vertexID = vertexID;
		this.environment = environment;
		this.initialCheckpointState = initialCheckpointState;
		this.taskManager = taskManager;

		this.environment.setExecutionObserver(this);
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		// Notify the listeners
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().userThreadStarted(this.environment.getJobID(), this.vertexID, userThread);
		}
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void markAsFailed() {

		executionStateChanged(ExecutionState.FAILED, "Execution thread died unexpectedly");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cancelExecution() {

		cancelOrKillExecution(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killExecution() {

		cancelOrKillExecution(false);
	}

	/**
	 * Cancels or kills the task.
	 * 
	 * @param cancel
	 *        <code>true/code> if the task shall be canceled, <code>false</code> if it shall be killed
	 */
	private void cancelOrKillExecution(final boolean cancel) {

		final Thread executingThread = this.environment.getExecutingThread();

		if (executingThread == null) {
			return;
		}

		if (this.executionState != ExecutionState.RUNNING && this.executionState != ExecutionState.REPLAYING
			&& this.executionState != ExecutionState.FINISHING) {
			return;
		}

		LOG.info((cancel ? "Canceling " : "Killing ") + this.environment.getTaskNameWithIndex());

		if (cancel) {
			this.isCanceled = true;
			// Change state
			executionStateChanged(ExecutionState.CANCELING, null);

			// Request user code to shut down
			try {
				final AbstractInvokable invokable = this.environment.getInvokable();
				if (invokable != null) {
					invokable.cancel();
				}
			} catch (Throwable e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		}

		// Continuously interrupt the user thread until it changed to state CANCELED
		while (true) {

			executingThread.interrupt();

			if (!executingThread.isAlive()) {
				break;
			}

			try {
				executingThread.join(1000);
			} catch (InterruptedException e) {}
			
			if (!executingThread.isAlive()) {
				break;
			}

			if (LOG.isDebugEnabled())
				LOG.debug("Sending repeated " + (cancel == true ? "canceling" : "killing") + " signal to " +
					this.environment.getTaskName() + " with state " + this.executionState);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void startExecution() {

		final Thread thread = this.environment.getExecutingThread();
		thread.start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isCanceled() {

		return this.isCanceled;
	}

	public void checkpointStateChanged(final CheckpointState newCheckpointState) {

		// Propagate event to the job manager
		this.taskManager.checkpointStateChanged(this.environment.getJobID(), this.vertexID, newCheckpointState);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isTerminated() {

		final Thread executingThread = this.environment.getExecutingThread();
		if (executingThread.getState() == Thread.State.TERMINATED) {
			return true;
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Environment getEnvironment() {

		return this.environment;
	}

	/**
	 * Returns the runtime environment associated with this task.
	 * 
	 * @return the runtime environment associated with this task
	 */
	public RuntimeEnvironment getRuntimeEnvironment() {

		return this.environment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.environment.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerProfiler(final TaskManagerProfiler taskManagerProfiler, final Configuration jobConfiguration) {

		taskManagerProfiler.registerExecutionListener(this, jobConfiguration);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterMemoryManager(final MemoryManager memoryManager) {

		if (memoryManager != null) {
			memoryManager.releaseAll(this.environment.getInvokable());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterProfiler(final TaskManagerProfiler taskManagerProfiler) {

		if (taskManagerProfiler != null) {
			taskManagerProfiler.unregisterExecutionListener(this.vertexID);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TaskContext createTaskContext(final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final LocalBufferPoolOwner previousBufferPoolOwner) {

		if (previousBufferPoolOwner != null) {
			throw new IllegalStateException("Vertex " + this.vertexID + " has a previous buffer pool owner");
		}

		return new RuntimeTaskContext(this, this.initialCheckpointState, transferEnvelopeDispatcher);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionState getExecutionState() {

		return this.executionState;
	}

	/**
	 * Registers a checkpoint decision requester object with this task.
	 * 
	 * @param checkpointDecisionRequester
	 *        the checkpoint decision requester object to register
	 */
	void registerCheckpointDecisionRequester(final CheckpointDecisionRequester checkpointDecisionRequester) {
		this.checkpointDecisionRequester = checkpointDecisionRequester;
	}

	/**
	 * Requests a checkpoint decision from the task.
	 * 
	 * @return <code>true</code> if the operation was successful, <code>false</code> if the task has not yet created a
	 *         checkpoint
	 */
	public boolean requestCheckpointDecision() {

		if (this.checkpointDecisionRequester == null) {
			return false;
		}

		LOG.info("Requesting checkpoint decision for task " + this.environment.getTaskNameWithIndex());

		this.checkpointDecisionRequester.requestCheckpointDecision();

		return true;
	}
}
