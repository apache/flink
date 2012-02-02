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

package eu.stratosphere.nephele.checkpointing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTaskContext;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.template.InputSplitProvider;

public final class ReplayTask implements Task {

	private final class ReplayTaskExecutionObserver implements ExecutionObserver {

		private final RuntimeTask encapsulatedTask;

		private ReplayTaskExecutionObserver(final RuntimeTask encapsulatedTask) {
			this.encapsulatedTask = encapsulatedTask;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void executionStateChanged(final ExecutionState newExecutionState, final String optionalMessage) {

			System.out.println("Execution state changed to " + newExecutionState + ", " + optionalMessage);
			
			if (this.encapsulatedTask == null) {
				replayTaskExecutionState = newExecutionState;
			} else {
				encapsulatedExecutionState = newExecutionState;
			}

			reportExecutionStateChange((this.encapsulatedTask == null), optionalMessage);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadStarted(final Thread userThread) {

			if (this.encapsulatedTask != null) {
				this.encapsulatedTask.userThreadStarted(userThread);
			} else {
				LOG.error("userThreadStarted called although there is no encapsulated task");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadFinished(final Thread userThread) {

			if (this.encapsulatedTask != null) {
				this.encapsulatedTask.userThreadFinished(userThread);
			} else {
				LOG.error("userThreadFinished called although there is no encapsulated task");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean isCanceled() {

			if (this.encapsulatedTask != null) {
				if (this.encapsulatedTask.isCanceled()) {
					return true;
				}
			}

			return isCanceled;
		}
	}

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ReplayTask.class);

	private final ExecutionVertexID vertexID;

	private final CheckpointEnvironment environment;

	private final RuntimeTask encapsulatedTask;

	private final TaskManager taskManager;

	private volatile ExecutionState encapsulatedExecutionState = null;

	private volatile ExecutionState replayTaskExecutionState = ExecutionState.STARTING;

	private AtomicBoolean replayThreadStarted = new AtomicBoolean(false);

	/**
	 * Stores whether the task has been canceled.
	 */
	private volatile boolean isCanceled = false;

	private final Map<ChannelID, ReplayOutputBroker> outputBrokerMap = new ConcurrentHashMap<ChannelID, ReplayOutputBroker>();

	public ReplayTask(final ExecutionVertexID vertexID, final Environment environment,
			final TaskManager taskManager) {

		this.vertexID = vertexID;
		this.environment = new CheckpointEnvironment(this.vertexID, environment,
			CheckpointUtils.hasCompleteCheckpointAvailable(vertexID), this.outputBrokerMap);
		this.environment.setExecutionObserver(new ReplayTaskExecutionObserver(null));

		this.encapsulatedTask = null;
		this.taskManager = taskManager;
	}

	public ReplayTask(final RuntimeTask encapsulatedTask, final TaskManager taskManager) {

		this.vertexID = encapsulatedTask.getVertexID();
		this.environment = new CheckpointEnvironment(this.vertexID, encapsulatedTask.getEnvironment(),
			CheckpointUtils.hasCompleteCheckpointAvailable(vertexID), this.outputBrokerMap);
		this.environment.setExecutionObserver(new ReplayTaskExecutionObserver(null));

		this.encapsulatedTask = encapsulatedTask;
		// Redirect all state change notifications to this task
		this.encapsulatedTask.getRuntimeEnvironment().setExecutionObserver(
			new ReplayTaskExecutionObserver(this.encapsulatedTask));
		this.encapsulatedExecutionState = this.encapsulatedTask.getExecutionState();
		this.taskManager = taskManager;
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
	public Environment getEnvironment() {

		return this.environment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void markAsFailed() {

		this.replayTaskExecutionState = ExecutionState.FAILED;
		reportExecutionStateChange(true, "Execution thread died unexpectedly");
	}

	@Override
	public boolean isTerminated() {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void startExecution() {

		final ReplayThread thread = this.environment.getExecutingThread();
		if (this.replayThreadStarted.compareAndSet(false, true)) {
			thread.start();
		} else {
			thread.restart();
		}
	}

	@Override
	public void cancelExecution() {
		// TODO Auto-generated method stub

	}

	@Override
	public void killExecution() {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerMemoryManager(final MemoryManager memoryManager) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerIOManager(final IOManager ioManager) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputSplitProvider(final InputSplitProvider inputSplitProvider) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerProfiler(final TaskManagerProfiler taskManagerProfiler, final Configuration jobConfiguration) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterMemoryManager(final MemoryManager memoryManager) {

		if (this.encapsulatedTask != null) {
			this.encapsulatedTask.unregisterMemoryManager(memoryManager);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterProfiler(final TaskManagerProfiler taskManagerProfiler) {

		if (this.encapsulatedTask != null) {
			this.encapsulatedTask.unregisterProfiler(taskManagerProfiler);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TaskContext createTaskContext(final TransferEnvelopeDispatcher transferEnvelopeDispatcher,
			final Map<ExecutionVertexID, RuntimeTaskContext> tasksWithUndecidedCheckpoints) {

		return new ReplayTaskContext(this, transferEnvelopeDispatcher);
	}

	private void reportExecutionStateChange(final boolean replayTaskStateChanged, final String optionalMessage) {

		final JobID jobID = this.environment.getJobID();

		if (replayTaskStateChanged) {

			if (this.replayTaskExecutionState == ExecutionState.REPLAYING) {
				this.taskManager.executionStateChanged(jobID, this.vertexID, this.replayTaskExecutionState,
					optionalMessage);
			}

		} else {

		}
	}

	void registerReplayOutputBroker(final ChannelID channelID, final ReplayOutputBroker outputBroker) {

		this.outputBrokerMap.put(channelID, outputBroker);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionState getExecutionState() {

		return null;
	}

}
