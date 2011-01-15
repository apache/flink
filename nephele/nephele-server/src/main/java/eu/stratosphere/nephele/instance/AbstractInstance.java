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

package eu.stratosphere.nephele.instance;

import java.io.IOException;
import java.net.InetSocketAddress;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileRequest;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheProfileResponse;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheUpdate;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelConfigurator;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.TaskOperationProtocol;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * An abstract instance represents a resource a {@link eu.stratosphere.nephele.taskmanager.TaskManager} runs on.
 * 
 * @author warneke
 */
public abstract class AbstractInstance extends NetworkNode implements ChannelConfigurator {

	/**
	 * The type of the instance.
	 */
	private final InstanceType instanceType;

	/**
	 * The connection info identifying the instance.
	 */
	private final InstanceConnectionInfo instanceConnectionInfo;

	/**
	 * The hardware description as reported by the instance itself.
	 */
	private final HardwareDescription hardwareDescription;

	/**
	 * Stores the RPC stub object for the instance's task manager.
	 */
	private TaskOperationProtocol taskManager = null;

	/**
	 * Constructs an abstract instance object.
	 * 
	 * @param instanceType
	 *        the type of the instance
	 * @param instanceConnectionInfo
	 *        the connection info identifying the instance
	 * @param parentNode
	 *        the parent node in the network topology
	 * @param networkTopology
	 *        the network topology this node is a part of
	 * @param hardwareDescription
	 *        the hardware description provided by the instance itself
	 */
	public AbstractInstance(InstanceType instanceType, InstanceConnectionInfo instanceConnectionInfo,
			NetworkNode parentNode, NetworkTopology networkTopology, HardwareDescription hardwareDescription) {
		super((instanceConnectionInfo == null) ? null : instanceConnectionInfo.toString(), parentNode, networkTopology);
		this.instanceType = instanceType;
		this.instanceConnectionInfo = instanceConnectionInfo;
		this.hardwareDescription = hardwareDescription;
	}

	/**
	 * Creates or returns the RPC stub object for the instance's task manager.
	 * 
	 * @return the RPC stub object for the instance's task manager
	 * @throws IOException
	 *         thrown if the RPC stub object for the task manager cannot be created
	 */
	protected TaskOperationProtocol getTaskManager() throws IOException {

		if (this.taskManager == null) {

			this.taskManager = (TaskOperationProtocol) RPC.getProxy(TaskOperationProtocol.class, new InetSocketAddress(
				getInstanceConnectionInfo().getAddress(), getInstanceConnectionInfo().getIPCPort()), NetUtils
				.getSocketFactory());
		}

		return this.taskManager;
	}

	/**
	 * Returns the type of the instance.
	 * 
	 * @return the type of the instance
	 */
	public final InstanceType getType() {
		return this.instanceType;
	}

	/**
	 * Returns the instance's connection information object.
	 * 
	 * @return the instance's connection information object
	 */
	public final InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	/**
	 * Returns the instance's hardware description as reported by the instance itself.
	 * 
	 * @return the instance's hardware description
	 */
	public HardwareDescription getHardwareDescription() {
		return this.hardwareDescription;
	}

	/**
	 * Checks if all the libraries required to run the job with the given
	 * job ID are available on this instance. Any libary that is missing
	 * is transferred to the instance as a result of this call.
	 * 
	 * @param jobID
	 *        the ID of the job whose libraries are to be checked for
	 * @throws IOException
	 *         thrown if an error occurs while checking for the libraries
	 */
	public synchronized void checkLibraryAvailability(JobID jobID) throws IOException {

		// Now distribute the required libraries for the job
		String[] requiredLibraries = LibraryCacheManager.getRequiredJarFiles(jobID);

		if (requiredLibraries == null)
			throw new IOException("No entry of required libraries for job " + jobID);

		LibraryCacheProfileRequest request = new LibraryCacheProfileRequest();
		request.setRequiredLibraries(requiredLibraries);

		// Send the request
		LibraryCacheProfileResponse response = null;
		response = getTaskManager().getLibraryCacheProfile(request);

		// Check response and transfer libaries if necesarry
		for (int k = 0; k < requiredLibraries.length; k++) {
			if (!response.isCached(k)) {
				LibraryCacheUpdate update = new LibraryCacheUpdate(requiredLibraries[k]);
				getTaskManager().updateLibraryCache(update);
			}
		}
	}

	/**
	 * Submits the task represented by the given {@link Environment} object to the instance's
	 * {@link eu.stratosphere.nephele.taskmanager.TaskManager}.
	 * 
	 * @param id
	 *        the ID of the vertex to be submitted
	 * @param jobConfiguration
	 *        the configuration of the overall job
	 * @param environment
	 *        the environment encapsulating the task
	 * @return the result of the submission attempt
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the task
	 */
	public synchronized TaskSubmissionResult submitTask(ExecutionVertexID id, Configuration jobConfiguration,
			Environment environment) throws IOException {

		return getTaskManager().submitTask(id, jobConfiguration, environment);
	}

	/**
	 * Cancels the task identified by the given ID at the instance's
	 * {@link eu.stratosphere.nephele.taskmanager.TaskManager}.
	 * 
	 * @param id
	 *        the ID identifying the task to be canceled
	 * @return the result of the cancel attempt
	 */
	public synchronized TaskCancelResult cancelTask(ExecutionVertexID id) throws IOException {

		return getTaskManager().cancelTask(id);
	}

}
