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

package eu.stratosphere.pact.testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.CheckpointDecision;
import eu.stratosphere.nephele.checkpointing.CheckpointReplayResult;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.TaskOperationProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionWrapper;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * Mocks the {@link TaskManager} without building up any network connections. It supports memory and file channels for
 * an execution graph.
 * 
 * @author Arvid Heise
 */
class MockTaskManager implements TaskOperationProtocol {
	/**
	 * @author Arvid Heise
	 */
	private final class TaskObserver implements ExecutionObserver {
		/**
		 * 
		 */
		private final ExecutionVertexID id;

		/**
		 * 
		 */
		private final Environment environment;

		private volatile boolean isCanceled = false;

		/**
		 * Initializes ExecutionObserver.
		 * 
		 * @param id
		 * @param environment
		 */
		private TaskObserver(ExecutionVertexID id, Environment environment) {
			this.id = id;
			this.environment = environment;
		}

		@Override
		public void userThreadStarted(Thread userThread) {
		}

		@Override
		public void userThreadFinished(Thread userThread) {
		}

		@Override
		public boolean isCanceled() {
			return isCanceled;
		}

		@Override
		public void executionStateChanged(final ExecutionState executionState, final String optionalMessage) {
			// // Finally propagate the state change to the job manager
			// MockTaskManager. this.executionStateChanged(environment.getJobID(), id, this, newExecutionState,
			// optionalMessage);

			final ExecutionGraph eg = MockTaskManager.INSTANCE.jobGraphs.get(this.environment.getJobID());
			if (eg == null) {
				LOG.error("Cannot find execution graph for ID " + this.environment.getJobID() + " to change state to "
					+ executionState);
				return;
			}

			final ExecutionVertex vertex = eg.getVertexByID(this.id);
			if (vertex == null) {
				LOG.error("Cannot find vertex with ID " + this.id + " of job " + eg.getJobID()
					+ " to change state to " + executionState);
				return;
			}

			if (executionState == ExecutionState.FINISHED || executionState == ExecutionState.CANCELED
				|| executionState == ExecutionState.FAILED)
				MockTaskManager.this.channelManager.unregisterChannels(this.environment);

			if (executionState == ExecutionState.CANCELED)
				isCanceled = true;

			final Runnable taskStateChangeRunnable = new Runnable() {
				@Override
				public void run() {
					// The registered listeners of the vertex will make sure the appropriate actions are taken
					vertex.updateExecutionState(executionState, optionalMessage);
				}
			};
			ConcurrentUtil.invokeLater(taskStateChangeRunnable);

			eg.checkAndUpdateJobStatus(executionState);
		}
	}

	private static final Log LOG = LogFactory.getLog(MockTaskManager.class);

	private static final long MEMORY_SIZE = Math.max(192 << 20, Runtime.getRuntime().maxMemory() / 2);

	public static final MockTaskManager INSTANCE = new MockTaskManager();

	private MockChannelManager channelManager = new MockChannelManager();

	private final IOManager ioManager;

	private volatile MemoryManager memoryManager;

	private Map<JobID, ExecutionGraph> jobGraphs = new HashMap<JobID, ExecutionGraph>();

	private final Map<ExecutionVertexID, Environment> runningTasks = new HashMap<ExecutionVertexID, Environment>();

	private MockTaskManager() {
		// 256 mb
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, (int) (MEMORY_SIZE / 10));
		// this.memoryManager = new MockMemoryManager();
		// Initialize the io manager
		final String tmpDirPath = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		this.ioManager = new IOManager(tmpDirPath);
	}

	@Override
	public TaskCancelResult cancelTask(final ExecutionVertexID id) throws IOException {

		Environment environment = this.runningTasks.get(id);
		final Thread executingThread = environment.getExecutingThread();

		this.channelManager.unregisterChannels(environment);
		// Request user code to shut down
		try {
			final AbstractInvokable invokable = environment.getInvokable();
			if (invokable != null) {
				invokable.cancel();
			}
			executingThread.interrupt();
		} catch (Throwable e) {
			LOG.error(StringUtils.stringifyException(e));
		}

		return new TaskCancelResult(id, TaskCancelResult.ReturnCode.SUCCESS);
	}

	/**
	 * Returns the {@link IOManager}.
	 * 
	 * @return the IOManager
	 */
	public IOManager getIoManager() {
		return this.ioManager;
	}

	@Override
	public LibraryCacheProfileResponse getLibraryCacheProfile(final LibraryCacheProfileRequest request)
			throws IOException {
		final LibraryCacheProfileResponse response = new LibraryCacheProfileResponse(request);
		final String[] requiredLibraries = request.getRequiredLibraries();

		for (int i = 0; i < requiredLibraries.length; i++)
			response.setCached(i, true);

		return response;
	}

	/**
	 * Returns the {@link MemoryManager}.
	 * 
	 * @return the MemoryManager
	 */
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	@Override
	public void updateLibraryCache(final LibraryCacheUpdate update) throws IOException {
	}

	@Override
	public void removeCheckpoints(List<ExecutionVertexID> listOfVertexIDs) throws IOException {
	}

	@Override
	public void logBufferUtilization() throws IOException {
	}

	@Override
	public List<TaskSubmissionResult> submitTasks(final List<TaskSubmissionWrapper> tasks) throws IOException {

		final List<TaskSubmissionResult> resultList = new ArrayList<TaskSubmissionResult>();

		for (final TaskSubmissionWrapper tsw : tasks) {
			ExecutionVertexID id = tsw.getVertexID();
			Environment environment = tsw.getEnvironment();

			// Register task manager components in environment
			environment.setMemoryManager(this.memoryManager);
			environment.setIOManager(this.ioManager);

			environment.setExecutionObserver(new TaskObserver(id, environment));

			this.channelManager.registerChannels(environment);
			this.runningTasks.put(id, environment);
		}

		for (final TaskSubmissionWrapper tsw : tasks) {
			final Thread thread = tsw.getEnvironment().getExecutingThread();
			thread.start();
			resultList.add(new TaskSubmissionResult(tsw.getVertexID(), AbstractTaskResult.ReturnCode.SUCCESS));
		}

		return resultList;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#submitTask(eu.stratosphere.nephele.executiongraph.
	 * ExecutionVertexID, eu.stratosphere.nephele.configuration.Configuration,
	 * eu.stratosphere.nephele.execution.Environment, java.util.Set)
	 */
	@Override
	public TaskSubmissionResult submitTask(final ExecutionVertexID id, Configuration jobConfiguration,
			final Environment environment, Set<ChannelID> activeOutputChannels) throws IOException {
		// Register task manager components in environment
		environment.setMemoryManager(this.memoryManager);
		environment.setIOManager(this.ioManager);
		environment.setExecutionObserver(new TaskObserver(id, environment));

		this.channelManager.registerChannels(environment);
		this.runningTasks.put(id, environment);

		final Thread thread = environment.getExecutingThread();
		thread.start();

		return new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#replayCheckpoints(java.util.List)
	 */
	@Override
	public List<CheckpointReplayResult> replayCheckpoints(List<ExecutionVertexID> vertexIDs) throws IOException {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#propagateCheckpointDecisions(java.util.List)
	 */
	@Override
	public void propagateCheckpointDecisions(List<CheckpointDecision> checkpointDecisions) throws IOException {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#killTaskManager()
	 */
	@Override
	public void killTaskManager() throws IOException {
	}

	public void addJobGraph(ExecutionGraph eg) {
		this.jobGraphs.put(eg.getJobID(), eg);
	}
}
