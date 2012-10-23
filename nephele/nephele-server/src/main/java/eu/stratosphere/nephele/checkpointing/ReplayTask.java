/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.concurrent.atomic.AtomicReference;

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
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TaskContext;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeDispatcher;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.util.StringUtils;

public final class ReplayTask implements Task {

	private final class ReplayTaskExecutionObserver implements ExecutionObserver {

		private final RuntimeTask encapsulatedRuntimeTask;

		private ReplayTaskExecutionObserver(final RuntimeTask encapsulatedRuntimeTask) {
			this.encapsulatedRuntimeTask = encapsulatedRuntimeTask;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void executionStateChanged(final ExecutionState newExecutionState, final String optionalMessage) {

			if (this.encapsulatedRuntimeTask == null) {
				replayTaskExecutionState = newExecutionState;

				if (newExecutionState == ExecutionState.FAILED) {
					if (encapsulatedTask != null) {
						encapsulatedTask.killExecution();
					}
				}

			} else {
				encapsulatedExecutionState = newExecutionState;

				if (newExecutionState == ExecutionState.FAILED) {
					killExecution();
				}
			}

			reportExecutionStateChange((this.encapsulatedRuntimeTask == null), optionalMessage);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadStarted(final Thread userThread) {

			if (this.encapsulatedRuntimeTask != null) {
				this.encapsulatedRuntimeTask.userThreadStarted(userThread);
			} else {
				LOG.error("userThreadStarted called although there is no encapsulated task");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadFinished(final Thread userThread) {

			if (this.encapsulatedRuntimeTask != null) {
				this.encapsulatedRuntimeTask.userThreadFinished(userThread);
			} else {
				LOG.error("userThreadFinished called although there is no encapsulated task");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean isCanceled() {

			if (this.encapsulatedRuntimeTask != null) {
				if (this.encapsulatedRuntimeTask.isCanceled()) {
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

	private final AtomicReference<ExecutionState> overallExecutionState = new AtomicReference<ExecutionState>(
		ExecutionState.STARTING);

	private final AtomicBoolean replayThreadStarted = new AtomicBoolean(false);

	/**
	 * Stores whether the task has been canceled.
	 */
	private volatile boolean isCanceled = false;

	private final Map<ChannelID, ReplayOutputChannelBroker> outputBrokerMap = new ConcurrentHashMap<ChannelID, ReplayOutputChannelBroker>();

	public ReplayTask(final ExecutionVertexID vertexID, final Environment environment,
			final TaskManager taskManager) {

		this.vertexID = vertexID;

		this.environment = new CheckpointEnvironment(this.vertexID, environment,
			CheckpointUtils.hasLocalCheckpointAvailable(this.vertexID),
			CheckpointUtils.hasCompleteCheckpointAvailable(this.vertexID),
			this.outputBrokerMap);

		this.environment.setExecutionObserver(new ReplayTaskExecutionObserver(null));

		this.encapsulatedTask = null;
		this.taskManager = taskManager;
	}

	public ReplayTask(final RuntimeTask encapsulatedTask, final TaskManager taskManager) {

		this.vertexID = encapsulatedTask.getVertexID();

		this.environment = new CheckpointEnvironment(this.vertexID, encapsulatedTask.getEnvironment(),
			CheckpointUtils.hasLocalCheckpointAvailable(this.vertexID),
			CheckpointUtils.hasCompleteCheckpointAvailable(this.vertexID),
			this.outputBrokerMap);

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

		if (this.encapsulatedTask != null) {
			this.encapsulatedTask.killExecution();
		}
		this.replayTaskExecutionState = ExecutionState.FAILED;
		reportExecutionStateChange(true, "Execution thread died unexpectedly");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isTerminated() {

		if (this.encapsulatedTask != null) {
			if (this.encapsulatedTask.isTerminated()) {

				if (this.encapsulatedExecutionState != ExecutionState.FINISHED
					&& this.encapsulatedExecutionState != ExecutionState.CANCELED
					&& this.encapsulatedExecutionState != ExecutionState.FAILED) {

					return true;
				}
			}
		}

		final Thread executingThread = this.environment.getExecutingThread();
		if (executingThread.getState() == Thread.State.TERMINATED) {

			if (this.replayTaskExecutionState != ExecutionState.FINISHED
				&& this.replayTaskExecutionState != ExecutionState.CANCELED
				&& this.replayTaskExecutionState != ExecutionState.FAILED) {

				return true;
			}
		}

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
	 *        <code>true/code> if the task shall be cancelled, <code>false</code> if it shall be killed
	 */
	private void cancelOrKillExecution(final boolean cancel) {

		final Thread replayThread = this.environment.getExecutingThread();
		Thread encapsulatedThread = null;
		if (this.encapsulatedTask != null) {
			encapsulatedThread = this.encapsulatedTask.getRuntimeEnvironment().getExecutingThread();
		}

		if (replayThread == null && encapsulatedThread == null) {
			return;
		}

		if (cancel) {
			this.isCanceled = true;
			this.replayTaskExecutionState = ExecutionState.CANCELING;
			if (this.encapsulatedExecutionState != null) {
				this.encapsulatedExecutionState = ExecutionState.CANCELING;
			}

			reportExecutionStateChange(true, null);

			// Request user code to shut down
			if (this.encapsulatedTask != null) {

				try {
					final AbstractInvokable invokable = this.encapsulatedTask.getRuntimeEnvironment().getInvokable();
					if (invokable != null) {
						invokable.cancel();
					}
				} catch (Throwable e) {
					LOG.error(StringUtils.stringifyException(e));
				}
			}
		}

		// Continuously interrupt the threads until it changed to state CANCELED
		while (true) {

			replayThread.interrupt();
			if (encapsulatedThread != null) {
				encapsulatedThread.interrupt();
			}

			if (cancel) {
				if (this.overallExecutionState.get() == ExecutionState.CANCELED) {
					break;
				}
			} else {
				if (this.overallExecutionState.get() == ExecutionState.FAILED) {
					break;
				}
			}

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				break;
			}
		}
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
			final LocalBufferPoolOwner previousBufferPoolOwner) {

		return new ReplayTaskContext(this, transferEnvelopeDispatcher, previousBufferPoolOwner, this.environment
			.getOutputChannelIDs().size());
	}

	private void reportExecutionStateChange(final boolean replayTaskStateChanged, final String optionalMessage) {

		ExecutionState candidateState;
		if (replayTaskStateChanged) {
			candidateState = determineOverallExecutionState(this.encapsulatedExecutionState,
				this.replayTaskExecutionState);
		} else {
			candidateState = determineOverallExecutionState(this.replayTaskExecutionState,
				this.encapsulatedExecutionState);
		}

		if (candidateState == null) {
			return;
		}

		if (this.overallExecutionState.getAndSet(candidateState) != candidateState) {
			this.taskManager.executionStateChanged(this.environment.getJobID(), this.vertexID, candidateState,
				optionalMessage);
		}
	}

	void registerReplayOutputBroker(final ChannelID channelID, final ReplayOutputChannelBroker outputBroker) {

		this.outputBrokerMap.put(channelID, outputBroker);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionState getExecutionState() {

		return null;
	}

	private static ExecutionState determineOverallExecutionState(final ExecutionState unchangedExecutionState,
			final ExecutionState changedExecutionState) {

		if (unchangedExecutionState == null) {
			return changedExecutionState;
		}

		if (changedExecutionState == ExecutionState.REPLAYING) {

			if (unchangedExecutionState == ExecutionState.RUNNING
				|| unchangedExecutionState == ExecutionState.FINISHING) {
				return ExecutionState.REPLAYING;
			} else {
				return unchangedExecutionState;
			}
		}

		if (changedExecutionState == ExecutionState.CANCELING) {
			return ExecutionState.CANCELING;
		}

		if (changedExecutionState == ExecutionState.CANCELED && unchangedExecutionState == ExecutionState.CANCELED) {
			return ExecutionState.CANCELED;
		}

		if (changedExecutionState == ExecutionState.FINISHING
			&& (unchangedExecutionState == ExecutionState.FINISHING || unchangedExecutionState == ExecutionState.FINISHED)) {
			return ExecutionState.FINISHING;
		}

		if (changedExecutionState == ExecutionState.FINISHED && unchangedExecutionState == ExecutionState.FINISHED) {
			return ExecutionState.FINISHED;
		}

		if (changedExecutionState == ExecutionState.FAILED && unchangedExecutionState == ExecutionState.FAILED) {
			return ExecutionState.FAILED;
		}

		return null;
	}
}
