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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import junit.framework.Assert;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * Mocks the localhost as an {@link AbstractInstance}.
 * 
 * @author Arvid Heise
 */
class MockInstance extends AbstractInstance {
	public final static HardwareDescription DESCRIPTION = HardwareDescriptionFactory
		.construct(1, 256 << 20, 128 << 20);

	// private final Map<ChannelID, String> fileNames = new HashMap<ChannelID,
	// String>();

	MockInstance(final InstanceType instanceType, final NetworkTopology networkTopology) {
		super(instanceType, createConnectionInfo(), null, networkTopology
			.getRootNode(), networkTopology, DESCRIPTION);
	}

	@Override
	public synchronized void checkLibraryAvailability(final JobID jobID)
			throws IOException {

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.nephele.instance.AbstractInstance#submitTasks(java.util.List)
	 */
	@Override
	public synchronized List<TaskSubmissionResult> submitTasks(List<TaskDeploymentDescriptor> tasks) throws IOException {
		return this.mockTaskManager.submitTasks(tasks);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.nephele.instance.AbstractInstance#cancelTask(eu.stratosphere.nephele.executiongraph.ExecutionVertexID
	 * )
	 */
	@Override
	public synchronized TaskCancelResult cancelTask(ExecutionVertexID id) throws IOException {
		return this.mockTaskManager.cancelTask(id);
	}

	//
	// @Override
	// public String getUniqueFilename(final ChannelID id) {
	// String name = this.fileNames.get(id);
	// if (name == null)
	// try {
	// final File file = File.createTempFile("mock", id.toString());
	// file.delete();
	// this.fileNames.put(id, name = file.getName());
	// } catch (final IOException e) {
	// }
	// return name;
	// }

	private MockTaskManager mockTaskManager = MockTaskManager.INSTANCE;

	private static InstanceConnectionInfo createConnectionInfo() {
		try {
			return new InstanceConnectionInfo(InetAddress.getLocalHost(), 1234, 2345);
		} catch (final UnknownHostException e) {
			Assert.fail(String.format("create connection info: %s", StringUtils.stringifyException(e)));
			return null;
		}
	}

}