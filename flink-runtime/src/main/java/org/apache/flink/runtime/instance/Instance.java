/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.instance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Collection;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheProfileRequest;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheProfileResponse;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheUpdate;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.apache.flink.runtime.taskmanager.TaskCancelResult;
import org.apache.flink.runtime.taskmanager.TaskKillResult;
import org.apache.flink.runtime.taskmanager.TaskSubmissionResult;

/**
 * An instance represents a resource a {@link org.apache.flink.runtime.taskmanager.TaskManager} runs on.
 */
public class Instance {
	
	/** The connection info to connect to the task manager represented by this instance. */
	private final InstanceConnectionInfo instanceConnectionInfo;
	
	/** A description of the resources of the task manager */
	private final HardwareDescription resources;
	
	/** The ID identifying the instance. */
	private final InstanceID instanceId;

	/** The number of task slots available on the node */
	private final int numberOfSlots;

	/**
	 * Allocated slots on this instance
	 */
	private final Map<AllocationID, AllocatedSlot> allocatedSlots = new HashMap<AllocationID, AllocatedSlot>();

	/**
	 * Stores the RPC stub object for the instance's task manager.
	 */
	private TaskOperationProtocol taskManager = null;

	/**
	 * Time when last heat beat has been received from the task manager running on this instance.
	 */
	private volatile long lastReceivedHeartBeat = System.currentTimeMillis();

	/**
	 * Constructs an abstract instance object.
	 * 
	 * @param instanceConnectionInfo The connection info under which to reach the TaskManager instance.
	 * @param id The id under which the instance is registered.
	 * @param resources The resources available on the machine.
	 * @param numberOfSlots The number of task slots offered by this instance.
	 */
	public Instance(InstanceConnectionInfo instanceConnectionInfo, InstanceID id, HardwareDescription resources, int numberOfSlots) {
		this.instanceConnectionInfo = instanceConnectionInfo;
		this.instanceId = id;
		this.resources = resources;
		this.numberOfSlots = numberOfSlots;
	}

	/**
	 * Creates or returns the RPC stub object for the instance's task manager.
	 * 
	 * @return the RPC stub object for the instance's task manager
	 * @throws IOException
	 *         thrown if the RPC stub object for the task manager cannot be created
	 */
	private TaskOperationProtocol getTaskManagerProxy() throws IOException {

		if (this.taskManager == null) {

			this.taskManager = RPC.getProxy(TaskOperationProtocol.class,
				new InetSocketAddress(getInstanceConnectionInfo().address(),
					getInstanceConnectionInfo().ipcPort()), NetUtils.getSocketFactory());
		}

		return this.taskManager;
	}

	/**
	 * Destroys and removes the RPC stub object for this instance's task manager.
	 */
	private void destroyTaskManagerProxy() {

		if (this.taskManager != null) {
			RPC.stopProxy(this.taskManager);
			this.taskManager = null;
		}
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
	 * Checks if all the libraries required to run the job with the given
	 * job ID are available on this instance. Any libary that is missing
	 * is transferred to the instance as a result of this call.
	 * 
	 * @param jobID
	 *        the ID of the job whose libraries are to be checked for
	 * @throws IOException
	 *         thrown if an error occurs while checking for the libraries
	 */
	public synchronized void checkLibraryAvailability(final JobID jobID) throws IOException {

		// Now distribute the required libraries for the job
		String[] requiredLibraries = LibraryCacheManager.getRequiredJarFiles(jobID);

		if (requiredLibraries == null) {
			throw new IOException("No entry of required libraries for job " + jobID);
		}

		LibraryCacheProfileRequest request = new LibraryCacheProfileRequest();
		request.setRequiredLibraries(requiredLibraries);

		// Send the request
		LibraryCacheProfileResponse response = null;
		response = getTaskManagerProxy().getLibraryCacheProfile(request);

		// Check response and transfer libraries if necessary
		for (int k = 0; k < requiredLibraries.length; k++) {
			if (!response.isCached(k)) {
				LibraryCacheUpdate update = new LibraryCacheUpdate(requiredLibraries[k]);
				getTaskManagerProxy().updateLibraryCache(update);
			}
		}
	}

	/**
	 * Submits a list of tasks to the instance's {@link org.apache.flink.runtime.taskmanager.TaskManager}.
	 * 
	 * @param tasks
	 *        the list of tasks to be submitted
	 * @return the result of the submission attempt
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the task
	 */
	public synchronized List<TaskSubmissionResult> submitTasks(final List<TaskDeploymentDescriptor> tasks) throws IOException {
		return getTaskManagerProxy().submitTasks(tasks);
	}

	/**
	 * Cancels the task identified by the given ID at the instance's
	 * {@link org.apache.flink.runtime.taskmanager.TaskManager}.
	 * 
	 * @param id
	 *        the ID identifying the task to be canceled
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the request or receiving the response
	 * @return the result of the cancel attempt
	 */
	public synchronized TaskCancelResult cancelTask(final ExecutionVertexID id) throws IOException {

		return getTaskManagerProxy().cancelTask(id);
	}

	/**
	 * Kills the task identified by the given ID at the instance's
	 * {@link org.apache.flink.runtime.taskmanager.TaskManager}.
	 * 
	 * @param id
	 *        the ID identifying the task to be killed
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the request or receiving the response
	 * @return the result of the kill attempt
	 */
	public synchronized TaskKillResult killTask(final ExecutionVertexID id) throws IOException {

		return getTaskManagerProxy().killTask(id);
	}

	/**
	 * Updates the time of last received heart beat to the current system time.
	 */
	public void reportHeartBeat() {
		this.lastReceivedHeartBeat = System.currentTimeMillis();
	}

	public boolean isStillAlive(long now, long cleanUpInterval) {
		return this.lastReceivedHeartBeat + cleanUpInterval > now;
	}


	@Override
	public boolean equals(final Object obj) {

		// Fall back since dummy instances do not have a instanceConnectionInfo
		if (this.instanceConnectionInfo == null) {
			return super.equals(obj);
		}

		if (!(obj instanceof Instance)) {
			return false;
		}

		final Instance abstractInstance = (Instance) obj;

		return this.instanceConnectionInfo.equals(abstractInstance.getInstanceConnectionInfo());
	}


	@Override
	public int hashCode() {

		// Fall back since dummy instances do not have a instanceConnectionInfo
		if (this.instanceConnectionInfo == null) {
			return super.hashCode();
		}

		return this.instanceConnectionInfo.hashCode();
	}

	/**
	 * Triggers the remote task manager to print out the current utilization of its read and write buffers to its logs.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the request
	 */
	public synchronized void logBufferUtilization() throws IOException {

		getTaskManagerProxy().logBufferUtilization();
	}

	/**
	 * Kills the task manager running on this instance. This method is mainly intended to test and debug Nephele's fault
	 * tolerance mechanisms.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the request
	 */
	public synchronized void killTaskManager() throws IOException {

		getTaskManagerProxy().killTaskManager();
	}

	/**
	 * Invalidates the entries identified by the given channel IDs from the remote task manager's receiver lookup cache.
	 * 
	 * @param channelIDs
	 *        the channel IDs identifying the cache entries to invalidate
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	public synchronized void invalidateLookupCacheEntries(final Set<ChannelID> channelIDs) throws IOException {
		getTaskManagerProxy().invalidateLookupCacheEntries(channelIDs);
	}

	/**
	 * Destroys all RPC stub objects attached to this instance.
	 */
	public synchronized void destroyProxies() {

		destroyTaskManagerProxy();

	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	public int getNumberOfAvailableSlots() { return numberOfSlots - allocatedSlots.size(); }

	public synchronized AllocatedResource allocateSlot(JobID jobID) throws InstanceException{
		if(allocatedSlots.size() < numberOfSlots){
			AllocatedSlot slot = new AllocatedSlot(jobID);

			allocatedSlots.put(slot.getAllocationID(), slot);
			return new AllocatedResource(this,slot.getAllocationID());
		}else{
			throw new InstanceException("Overbooking instance " + instanceConnectionInfo + ".");
		}
	}

	public synchronized void releaseSlot(AllocationID allocationID) {
		if(allocatedSlots.containsKey(allocationID)){
			allocatedSlots.remove(allocationID);
		}else{
			throw new RuntimeException("There is no slot registered with allocation ID " + allocationID + ".");
		}
	}

	public Collection<AllocatedSlot> getAllocatedSlots() {
		return allocatedSlots.values();
	}

	public Collection<AllocatedSlot> removeAllocatedSlots() {
		Collection<AllocatedSlot> slots = new ArrayList<AllocatedSlot>(this.allocatedSlots.values());

		for(AllocatedSlot slot : slots){
			releaseSlot(slot.getAllocationID());
		}

		return slots;
	}

	public long getLastHeartBeat() {
		return this.lastReceivedHeartBeat;
	}
	
	
	public void markDied() {
		
	}
	
	public void destroy() {
		
	}
	
	public InstanceID getId() {
		return instanceId;
	}
	
	public HardwareDescription getResources() {
		return this.resources;
	}
}
