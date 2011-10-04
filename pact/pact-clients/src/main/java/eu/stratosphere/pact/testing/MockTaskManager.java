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

import junit.framework.Assert;

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
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelSetupException;
import eu.stratosphere.nephele.io.channels.direct.AbstractDirectInputChannel;
import eu.stratosphere.nephele.io.channels.direct.AbstractDirectOutputChannel;
import eu.stratosphere.nephele.io.channels.direct.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.direct.InMemoryOutputChannel;
import eu.stratosphere.nephele.protocols.TaskOperationProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionWrapper;
import eu.stratosphere.nephele.taskmanager.direct.DirectChannelManager;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * Mocks the {@link TaskManager} without building up any network connections. It supports memory and file channels for
 * an
 * execution graph.
 * 
 * @author Arvid Heise
 */
class MockTaskManager implements TaskOperationProtocol {
	private static final long MEMORY_SIZE = Math.max(192 << 16, Runtime.getRuntime().maxMemory() / 2);

	public static final MockTaskManager INSTANCE = new MockTaskManager();

	/**
	 * The instance of the {@link DirectChannelManager} which is responsible for
	 * setting up and cleaning up the direct channels of the tasks.
	 */
	private final DirectChannelManager directChannelManager;

	private final IOManager ioManager;

	private volatile MemoryManager memoryManager;

	private MockTaskManager() {
		// 256 mb
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, (int) (MEMORY_SIZE / 10));
		// this.memoryManager = new MockMemoryManager();
		// Initialize the io manager
		final String tmpDirPath = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		this.ioManager = new IOManager(tmpDirPath);

		// Initialize the direct channel manager
		this.directChannelManager = new DirectChannelManager();
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
	 * @throws ChannelSetupException
	 *         thrown if one of the channels could not be set up correctly
	 */
	private void registerInputChannels(final InputGate<? extends Record> eig) throws ChannelSetupException {

		for (int i = 0; i < eig.getNumberOfInputChannels(); i++) {

			final AbstractInputChannel<? extends Record> eic = eig.getInputChannel(i);

			// if (eic instanceof NetworkInputChannel<?>) {
			// this.networkChannelManager.registerNetworkInputChannel((NetworkInputChannel<? extends Record>) eic);
			// } else
			// if (eic instanceof FileInputChannel<?>)
			// this.fileConnectionManager.registerFileInputChannel((FileInputChannel<? extends Record>) eic);
			// else
			if (eic instanceof InMemoryInputChannel<?>)
				this.directChannelManager
					.registerDirectInputChannel((AbstractDirectInputChannel<? extends Record>) eic);
			else
				throw new ChannelSetupException("Type of input channel " + eic.getType() + " is not supported");
		}
	}

	/**
	 * Registers the output channels of an incoming task with the task manager.
	 * 
	 * @param eig
	 *        the output gate whose input channels should be registered
	 * @throws ChannelSetupException
	 *         thrown if one of the channels could not be registered
	 */
	private void registerOutputChannels(final OutputGate<? extends Record> eog) throws ChannelSetupException {

		for (int i = 0; i < eog.getNumberOfOutputChannels(); i++) {

			final AbstractOutputChannel<? extends Record> eoc = eog.getOutputChannel(i);
			// if (eoc instanceof NetworkOutputChannel<?>) {
			// this.networkChannelManager.registerNetworkOutputChannel((NetworkOutputChannel<? extends Record>) eoc);
			// } else
			// if (eoc instanceof FileOutputChannel<?>)
			// this.fileConnectionManager.registerFileOutputChannel((FileOutputChannel<? extends Record>) eoc);
			// else
			if (eoc instanceof InMemoryOutputChannel<?>)
				this.directChannelManager
					.registerDirectOutputChannel((AbstractDirectOutputChannel<? extends Record>) eoc);
			else
				throw new ChannelSetupException("Type of output channel " + eoc.getType() + " is not supported");
		}
	}

	private void setupChannels(final ExecutionVertexID id, final Environment ee) {
		// Check if the task has unbound input/output gates
		if (ee.hasUnboundInputGates() || ee.hasUnboundOutputGates())
			Assert.fail(String.format("Task %s with ID %s has unbound gates", ee.getTaskName(), id));
		try {
			// Register input gates
			for (int i = 0; i < ee.getNumberOfInputGates(); i++)
				this.registerInputChannels(ee.getInputGate(i));
			// Register output gates
			for (int i = 0; i < ee.getNumberOfOutputGates(); i++)
				this.registerOutputChannels(ee.getOutputGate(i));
		} catch (final ChannelSetupException e) {
			Assert.fail(String.format("failed to setup channels @ %s: %s", ee.getTaskName(),
				StringUtils.stringifyException(e)));
		}
	}

	@Override
	public TaskSubmissionResult submitTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final Environment ee) throws IOException {
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

	@Override
	public void removeCheckpoints(List<ExecutionVertexID> listOfVertexIDs) throws IOException {
	}

	@Override
	public void logBufferUtilization() throws IOException {
	}

	@Override
	public List<TaskSubmissionResult> submitTasks(final List<TaskSubmissionWrapper> tasks) throws IOException {

		final List<TaskSubmissionResult> resultList = new ArrayList<TaskSubmissionResult>();

		for (final TaskSubmissionWrapper tsw : tasks)
			resultList.add(this.submitTask(tsw.getVertexID(), tsw.getConfiguration(), tsw.getEnvironment()));

		return resultList;
	}
}
