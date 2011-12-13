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

package eu.stratosphere.nephele.taskmanager;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.ExecutionStateTransition;
import eu.stratosphere.nephele.execution.ResourceUtilizationSnapshot;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public class Task implements ExecutionObserver {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(Task.class);

	private final ExecutionVertexID vertexID;

	private final Environment environment;

	private final TaskManager taskManager;

	/**
	 * Stores whether the task has been canceled.
	 */
	private volatile boolean isCanceled = false;

	/**
	 * The current execution state of the task
	 */
	private volatile ExecutionState executionState = ExecutionState.STARTING;

	private Queue<ExecutionListener> registeredListeners = new ConcurrentLinkedQueue<ExecutionListener>();

	Task(final ExecutionVertexID vertexID, final Environment environment, final TaskManager taskManager) {

		this.vertexID = vertexID;
		this.environment = environment;
		this.taskManager = taskManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(final ExecutionState newExecutionState, final String optionalMessage) {

		// Check the state transition
		ExecutionStateTransition.checkTransition(getTaskName(), this.executionState, newExecutionState);

		// Notify all listener objects
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().executionStateChanged(this.environment.getJobID(), this.vertexID, newExecutionState,
				optionalMessage);
		}

		// Store the new execution state
		this.executionState = newExecutionState;

		// Finally propagate the state change to the job manager
		this.taskManager.executionStateChanged(this.environment.getJobID(), this.vertexID, this,
			newExecutionState, optionalMessage);
	}

	/**
	 * Returns the name of the task associated with this observer object.
	 * 
	 * @return the name of the task associated with this observer object
	 */
	private String getTaskName() {

		return this.environment.getTaskName() + " (" + (environment.getIndexInSubtaskGroup() + 1) + "/"
			+ environment.getCurrentNumberOfSubtasks() + ")";
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
	 * Marks the task as failed and triggers the appropriate state changes.
	 */
	public void markAsFailed() {

		executionStateChanged(ExecutionState.FAILED, "Execution thread died unexpectedly");
	}

	/**
	 * Cancels the execution of the task (i.e. interrupts the execution thread).
	 */
	public void cancelExecution() {

		final Thread executingThread = this.environment.getExecutingThread();

		if (executingThread == null) {
			return;
		}

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

		// Continuously interrupt the user thread until it changed to state CANCELED
		while (true) {

			executingThread.interrupt();

			if (this.executionState == ExecutionState.CANCELED) {
				break;
			}

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	/**
	 * Starts the execution of this Nephele task.
	 */
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

	/**
	 * Triggers the notification that the task has run out of its initial execution resources.
	 */
	public void initialExecutionResourcesExhausted() {

//		if (this.environment.getExecutingThread() != Thread.currentThread()) {
//			throw new ConcurrentModificationException(
//				"initialExecutionResourcesExhausted must be called from the task that executes the user code");
//		}

		// Construct a resource utilization snapshot
		final long timestamp = System.currentTimeMillis();
		final Map<ChannelID, Long> outputChannelUtilization = new HashMap<ChannelID, Long>();

		for (int i = 0; i < this.environment.getNumberOfOutputGates(); ++i) {
			final OutputGate<? extends Record> outputGate = this.environment.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {
				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
				outputChannelUtilization.put(outputChannel.getID(),
					Long.valueOf(outputChannel.getAmountOfDataTransmitted()));
			}
		}

		final ResourceUtilizationSnapshot rus = new ResourceUtilizationSnapshot(timestamp, outputChannelUtilization);

		// Notify the listener objects
		final Iterator<ExecutionListener> it = this.registeredListeners.iterator();
		while (it.hasNext()) {
			it.next().initialExecutionResourcesExhausted(this.environment.getJobID(), this.vertexID, rus);
		}

		// Finally, propagate event to the job manager
		this.taskManager.initialExecutionResourcesExhausted(this.environment.getJobID(), this.vertexID, rus);
	}

	public void checkpointStateChanged(final CheckpointState newCheckpointState) {

		// Propagate event to the job manager
		this.taskManager.checkpointStateChanged(this.environment.getJobID(), this.vertexID, newCheckpointState);
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
	 * Returns the environment associated with this task.
	 * 
	 * @return the environment associated with this task
	 */
	public Environment getEnvironment() {

		return this.environment;
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
}
