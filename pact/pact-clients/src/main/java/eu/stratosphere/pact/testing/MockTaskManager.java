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
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.protocols.TaskOperationProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.CheckpointReplayResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionWrapper;
import eu.stratosphere.nephele.types.Record;

/**
 * Mocks the {@link TaskManager} without building up any network connections. It supports memory and file channels for
 * an
 * execution graph.
 * 
 * @author Arvid Heise
 */
class MockTaskManager implements TaskOperationProtocol {

	private final IOManager ioManager;

	private final MemoryManager memoryManager;

	MockTaskManager() {
		// 256 mb
		this.memoryManager = new DefaultMemoryManager(256 << 20);
		// Initialize the io manager
		final String tmpDirPath = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		this.ioManager = new IOManager(tmpDirPath);
	}

	@Override
	public TaskCancelResult cancelTask(final ExecutionVertexID id) throws IOException {
		return null;
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
	 * Returns the {@linknull MemoryManager}.
	 * 
	 * @return the MemoryManager
	 */
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	/**
	 * Registers the input channels of an incoming task with the task manager.
	 * 
	 * @param eig
	 *        the input gate whose input channels should be registered
	 */
	private void registerInputChannels(final InputGate<? extends Record> eig) {

		throw new RuntimeException("Type of input channel " + eig.getChannelType() + " is not supported");
	}

	/**
	 * Registers the output channels of an incoming task with the task manager.
	 * 
	 * @param eig
	 *        the output gate whose input channels should be registered
	 */
	private void registerOutputChannels(final OutputGate<? extends Record> eog) {

		throw new RuntimeException("Type of output channel " + eog.getChannelType() + " is not supported");
	}

	private void setupChannels(final ExecutionVertexID id, final Environment ee) {
		// Check if the task has unbound input/output gates
		if (ee.hasUnboundInputGates() || ee.hasUnboundOutputGates())
			TestPlan.fail("Task with ID " + id + " has unbound gates", ee, id);

		// Register input gates
		for (int i = 0; i < ee.getNumberOfInputGates(); i++)
			this.registerInputChannels(ee.getInputGate(i));
		// Register output gates
		for (int i = 0; i < ee.getNumberOfOutputGates(); i++)
			this.registerOutputChannels(ee.getOutputGate(i));

	}

	@Override
	public TaskSubmissionResult submitTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final Environment ee, Set<ChannelID> activeOutputChannels) throws IOException {
		// Register task manager components in environment
		ee.setMemoryManager(this.memoryManager);
		ee.setIOManager(this.ioManager);

		this.setupChannels(id, ee);

		ee.startExecution();

		return new TaskSubmissionResult(id, AbstractTaskResult.ReturnCode.SUCCESS);
	}

	@Override
	public void updateLibraryCache(final LibraryCacheUpdate update) throws IOException {
	}

	static final MockTaskManager INSTANCE = new MockTaskManager();

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
			resultList.add(submitTask(tsw.getVertexID(), tsw.getConfiguration(), tsw.getEnvironment(),
				tsw.getActiveOutputChannels()));
		}

		return resultList;
	}

	@Override
	public List<CheckpointReplayResult> replayCheckpoints(List<ExecutionVertexID> vertexIDs) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
