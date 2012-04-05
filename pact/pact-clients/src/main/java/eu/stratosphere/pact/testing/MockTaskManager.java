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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
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
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointResult;
import eu.stratosphere.nephele.taskmanager.TaskKillResult;
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
		private final RuntimeEnvironment environment;

		private volatile boolean isCanceled = false;

		/**
		 * Initializes ExecutionObserver.
		 * 
		 * @param id
		 * @param environment
		 */
		private TaskObserver(ExecutionVertexID id, RuntimeEnvironment environment) {
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
			return this.isCanceled;
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
				LOG.error("Cannot find vertex with ID " + this.id + " of job " + eg.getJobID() + " to change state to "
					+ executionState);
				return;
			}

			final Runnable taskStateChangeRunnable = new Runnable() {
				@Override
				public void run() {
					// The registered listeners of the vertex will make sure the appropriate actions are taken
					vertex.updateExecutionState(executionState, optionalMessage);
				}
			};
			ConcurrentUtil.invokeLater(taskStateChangeRunnable);

			eg.executionStateChanged(this.environment.getJobID(), this.id, executionState, optionalMessage);

			if (executionState == ExecutionState.CANCELED || executionState == ExecutionState.FINISHED
				|| executionState == ExecutionState.FAILED)
				MockTaskManager.this.finishedTasks.add(this.environment);
		}

		/**
		 * 
		 */
		public void cancel() {
			this.isCanceled = true;
			ConcurrentUtil.invokeLater(new Runnable() {
				@Override
				public void run() {
					TaskObserver.this.executionStateChanged(ExecutionState.CANCELING, null);
				}
			});
		}
	}

	private static final Log LOG = LogFactory.getLog(MockTaskManager.class);

	private static final long MEMORY_SIZE = Math.max(192 << 20, Runtime.getRuntime().maxMemory() / 2);

	private List<RuntimeEnvironment> finishedTasks = new ArrayList<RuntimeEnvironment>();

	public static final MockTaskManager INSTANCE = new MockTaskManager();

	private MockChannelManager channelManager = new MockChannelManager();

	private final IOManager ioManager;

	private volatile MemoryManager memoryManager;

	private Map<JobID, ExecutionGraph> jobGraphs = new HashMap<JobID, ExecutionGraph>();

	private final Map<ExecutionVertexID, RuntimeEnvironment> runningTasks =
		new HashMap<ExecutionVertexID, RuntimeEnvironment>();

	private final Map<Environment, TaskObserver> observers = new IdentityHashMap<Environment, TaskObserver>();

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

		RuntimeEnvironment environment = this.runningTasks.get(id);
		final Thread executingThread = environment.getExecutingThread();

		this.finishedTasks.add(environment);
		this.observers.get(environment).cancel();
		// Request user code to shut down
		try {
			final AbstractInvokable invokable = environment.getInvokable();
			if (invokable != null)
				invokable.cancel();
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
			RuntimeEnvironment environment = tsw.getEnvironment();

			// Register task manager components in environment
			environment.setMemoryManager(this.memoryManager);
			environment.setIOManager(this.ioManager);

			TaskObserver observer = new TaskObserver(id, environment);
			environment.setExecutionObserver(observer);

			this.channelManager.registerChannels(environment);
			this.runningTasks.put(id, environment);
			this.observers.put(environment, observer);
		}

		for (final TaskSubmissionWrapper tsw : tasks) {
			final Thread thread = tsw.getEnvironment().getExecutingThread();
			thread.start();
			resultList.add(new TaskSubmissionResult(tsw.getVertexID(), AbstractTaskResult.ReturnCode.SUCCESS));
		}

		return resultList;
	}

	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#submitTask(eu.stratosphere.nephele.executiongraph.
	// * ExecutionVertexID, eu.stratosphere.nephele.configuration.Configuration,
	// * eu.stratosphere.nephele.execution.Environment, java.util.Set)
	// */
	// @Override
	// public TaskSubmissionResult submitTask(final ExecutionVertexID id, Configuration jobConfiguration,
	// final RuntimeEnvironment environment, Set<ChannelID> activeOutputChannels) throws IOException {
	// // Register task manager components in environment
	// environment.setMemoryManager(this.memoryManager);
	// environment.setIOManager(this.ioManager);
	// TaskObserver observer = new TaskObserver(id, environment);
	// environment.setExecutionObserver(observer);
	//
	// this.channelManager.registerChannels(environment);
	// this.runningTasks.put(id, environment);
	// this.observers.put(environment, observer);
	//
	// final Thread thread = environment.getExecutingThread();
	// thread.start();
	//
	// return new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	// }

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

	/**
	 * @param executionGraph
	 */
	public void cleanupJob(@SuppressWarnings("unused") ExecutionGraph executionGraph) {
		for (RuntimeEnvironment task : this.finishedTasks) {
			this.channelManager.unregisterChannels(task);
			this.observers.remove(task);
		}
		this.finishedTasks.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#killTask(eu.stratosphere.nephele.executiongraph.
	 * ExecutionVertexID)
	 */
	@Override
	public TaskKillResult killTask(ExecutionVertexID id) throws IOException {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#requestCheckpointDecision(eu.stratosphere.nephele.
	 * executiongraph.ExecutionVertexID)
	 */
	@Override
	public TaskCheckpointResult requestCheckpointDecision(ExecutionVertexID id) throws IOException {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.protocols.TaskOperationProtocol#invalidateLookupCacheEntries(java.util.Set)
	 */
	@Override
	public void invalidateLookupCacheEntries(Set<ChannelID> channelIDs) throws IOException {
	}
}
