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

package eu.stratosphere.nephele.instance.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Representation of a host of a compute cluster.
 * <p>
 * This class is thread-safe.
 * 
 * @author Dominic Battre
 */
class ClusterInstance extends AbstractInstance {

	/**
	 * A map of slices allocated on this host
	 */
	private final Map<AllocationID, AllocatedSlice> allocatedSlices = new HashMap<AllocationID, AllocatedSlice>();

	/**
	 * The remaining capacity of this host that can be used by instances
	 */
	private InstanceType remainingCapacity;

	/** Time when last heat beat has been received from the task manager running on this instance */
	private long lastReceivedHeartBeat = System.currentTimeMillis();

	/** Filenames associated with channels for {@link #getUniqueFilename(ChannelID)} */
	private final Map<ChannelID, String> filenames = new HashMap<ChannelID, String>();

	/**
	 * Constructor.
	 * 
	 * @param instanceConnectionInfo
	 *        the instance connection info identifying the host
	 * @param capacity
	 *        capacity of this host
	 * @param parentNode
	 *        the parent node of this node in the network topology
	 * @param networkTopology
	 *        the network topology this node is part of
	 */
	public ClusterInstance(InstanceConnectionInfo instanceConnectionInfo, InstanceType capacity,
			NetworkNode parentNode, NetworkTopology networkTopology) {
		super(capacity, instanceConnectionInfo, parentNode, networkTopology);

		this.remainingCapacity = capacity;
	}

	/**
	 * Updates the time of last received heart beat to the current system time.
	 */
	synchronized void reportHeartBeat() {
		this.lastReceivedHeartBeat = System.currentTimeMillis();
	}

	/**
	 * Returns whether the host is still alive.
	 * 
	 * @param cleanUpInterval
	 *        duration (in milliseconds) after which a host is
	 *        considered dead if it has no received heat-beats.
	 * @return true if the host has received a heat-beat before the <code>cleanUpInterval</code> duration has expired.
	 */
	synchronized boolean isStillAlive(long cleanUpInterval) {
		if (this.lastReceivedHeartBeat + cleanUpInterval < System.currentTimeMillis()) {
			return false;
		}
		return true;
	}

	/**
	 * Tries to create a new slice on this instance
	 * 
	 * @param reqType
	 *        the type describing the hardware characteristics of the slice
	 * @param jobID
	 *        the ID of the job the new slice belongs to
	 * @return a new {@AllocatedSlice} object if a slice with the given hardware characteristics could
	 *         still be accommodated on this instance or <code>null</code> if the instance's remaining resources
	 *         were insufficient to host the desired slice
	 */
	synchronized AllocatedSlice createSlice(InstanceType reqType, JobID jobID) {

		// check whether we can accommodate the instance
		if (remainingCapacity.getNumberOfComputeUnits() >= reqType.getNumberOfComputeUnits()
			&& remainingCapacity.getNumberOfCores() >= reqType.getNumberOfCores()
			&& remainingCapacity.getMemorySize() >= reqType.getMemorySize()
			&& remainingCapacity.getDiskCapacity() >= reqType.getDiskCapacity()) {

			// reduce available capacity by what has been requested
			remainingCapacity = InstanceTypeFactory.construct(remainingCapacity.getIdentifier(), remainingCapacity
				.getNumberOfComputeUnits()
				- reqType.getNumberOfComputeUnits(), remainingCapacity.getNumberOfCores() - reqType.getNumberOfCores(),
				remainingCapacity.getMemorySize() - reqType.getMemorySize(), remainingCapacity.getDiskCapacity()
					- reqType.getDiskCapacity(), remainingCapacity.getPricePerHour());

			final long allocationTime = System.currentTimeMillis();

			final AllocatedSlice slice = new AllocatedSlice(this, reqType, jobID, allocationTime);
			this.allocatedSlices.put(slice.getAllocationID(), slice);
			return slice;
		}

		// we cannot accommodate the instance
		return null;
	}

	/**
	 * Removes the slice identified by the given allocation ID from
	 * this instance and frees up the allocated resources.
	 * 
	 * @param allocationID
	 *        the allocation ID of the slice to be removed
	 * @return the slice with has been removed from the instance or <code>null</code> if no slice
	 *         with the given allocation ID could be found
	 */
	synchronized AllocatedSlice removeAllocatedSlice(AllocationID allocationID) {

		final AllocatedSlice slice = this.allocatedSlices.remove(allocationID);
		if (slice != null) {

			this.remainingCapacity = InstanceTypeFactory.construct(this.remainingCapacity.getIdentifier(), this.remainingCapacity
				.getNumberOfComputeUnits()
				+ slice.getType().getNumberOfComputeUnits(), this.remainingCapacity.getNumberOfCores()
				+ slice.getType().getNumberOfCores(), this.remainingCapacity.getMemorySize()
				+ slice.getType().getMemorySize(), this.remainingCapacity.getDiskCapacity()
				+ slice.getType().getDiskCapacity(), this.remainingCapacity.getPricePerHour());
		}

		return slice;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUniqueFilename(ChannelID id) {

		synchronized (this.filenames) {

			if (this.filenames.containsKey(id))
				return this.filenames.get(id);

			// Simple implementation to generate a random filename
			char[] alphabet = { 'a', 'b', 'c', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

			String filename = "ne";

			for (int i = 0; i < 16; i++) {
				filename += alphabet[(int) (Math.random() * alphabet.length)];
			}

			filename += ".dat";
			// Store filename with id
			this.filenames.put(id, filename);

			return filename;
		}
	}

	/**
	 * Removes all allocated slices on this instance and frees
	 * up their allocated resources.
	 * 
	 * @return a list of all removed slices
	 */
	synchronized List<AllocatedSlice> removeAllAllocatedSlices() {

		final List<AllocatedSlice> slices = new ArrayList<AllocatedSlice>(this.allocatedSlices.values());
		final Iterator<AllocatedSlice> it = slices.iterator();
		while (it.hasNext()) {
			removeAllocatedSlice(it.next().getAllocationID());
		}

		return slices;
	}
}
