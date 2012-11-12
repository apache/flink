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

package eu.stratosphere.nephele.multicast;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.routing.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.taskmanager.routing.RemoteReceiver;

/**
 * The MulticastManager is responsible for the creation and storage of application-layer multicast trees used to
 * broadcast records to multiple target vertices.
 * 
 * @author casp
 * @author warneke
 */

public final class MulticastManager implements ChannelLookupProtocol {

	/**
	 * The log object used to report errors and warnings.
	 */
	private static final Log LOG = LogFactory.getLog(MulticastManager.class);

	/**
	 * Reference to the scheduler.
	 */
	private final AbstractScheduler scheduler;

	/**
	 * Map caching already computed multicast forwarding tables.
	 */
	private final ConcurrentMap<ChannelID, MulticastForwardingTable> cachedTrees = new ConcurrentHashMap<ChannelID, MulticastForwardingTable>();

	/**
	 * Constructs a new multicast manager.
	 * 
	 * @param scheduler
	 *        reference to the scheduler
	 */
	public MulticastManager(final AbstractScheduler scheduler) {

		this.scheduler = scheduler;
	}

	/**
	 * Retrieves all recipients of a data for the given <code>sourceChannelID</code>. Returns both local recipients as
	 * well as next-hop remote instances within the multicast-tree.
	 * 
	 * @param caller
	 *        the {@link InstanceConnectionInfo} object of the task manager which calls this method
	 * @param jobID
	 *        the ID of the job the channel ID belongs to
	 * @param sourceChannelID
	 *        the ID of the channel to resolve
	 * @return the lookup response containing the connection info and a return code
	 */
	public ConnectionInfoLookupResponse lookupConnectionInfo(final InstanceConnectionInfo caller,
			final JobID jobID, final ChannelID sourceChannelID) {

		// Check if the tree is already created and cached
		MulticastForwardingTable mft = this.cachedTrees.get(sourceChannelID);
		if (mft != null) {
			return mft.getConnectionInfo(caller);
		}

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			LOG.error("Cannot find execution graph with jod ID " + jobID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		final ExecutionEdge outputChannel = eg.getEdgeByID(sourceChannelID);
		if (outputChannel == null) {
			LOG.error("Cannot find execution edge with ID " + sourceChannelID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		// Perform a series of sanity checks
		if (!verifySource(caller, outputChannel)) {
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		// Check if all receivers are ready and do some sanity checks
		if (!checkIfAllTargetVerticesAreReady(outputChannel)) {
			LOG.info("Received multicast request but not all receivers ready.");
			return ConnectionInfoLookupResponse.createReceiverNotReady();
		}

		// Create new multicast forwarding table
		mft = createForwardingTable(outputChannel);

		if (LOG.isInfoEnabled()) {
			LOG.info(mft);
		}

		// Otherwise we create a default tree and put it into the tree-cache
		this.cachedTrees.put(sourceChannelID, mft);

		return mft.getConnectionInfo(caller);
	}

	private static boolean verifySource(final InstanceConnectionInfo caller, ExecutionEdge outputChannel) {

		// Sanity check
		if (!outputChannel.isBroadcast()) {
			LOG.error("Received multicast request tree from a non-broadcast edge");
			return false;
		}

		final ExecutionGate outputGate = outputChannel.getOutputGate();
		if (outputGate == null) {
			LOG.error("Cannot find output gate for output channel " + outputChannel.getOutputChannelID());
			return false;
		}

		final ExecutionVertex sourceVertex = outputGate.getVertex();
		if (sourceVertex == null) {
			LOG.error("Cannot find source vertex for output channel " + outputChannel.getOutputChannelID());
			return false;
		}

		final AbstractInstance instance = sourceVertex.getAllocatedResource().getInstance();
		if (instance == null) {
			LOG.error("Source vertex " + sourceVertex + " is not assigned to an instance");
			return false;
		}

		if (instance instanceof DummyInstance) {
			LOG.error("Source vertex " + sourceVertex + " is assigned to a dummy instance");
			return false;
		}

		if (!caller.equals(instance.getInstanceConnectionInfo())) {
			LOG.error(caller + " is not the source of the multicast tree");
			return false;
		}

		return true;
	}

	/**
	 * Checks, if all target vertices for a multicast transmission are ready. If vertices are in state ASSIGNED, those
	 * vertices will automatically be deployed.
	 * 
	 * @param outputChannel
	 *        the edge over which this multicast transmission is about to be initiated
	 */
	private boolean checkIfAllTargetVerticesAreReady(final ExecutionEdge outputChannel) {

		final ExecutionGate outputGate = outputChannel.getOutputGate();

		List<ExecutionVertex> verticesToDeploy = null;

		// Iterator over all broadcast edges and check the receivers' state.
		final int numberOfOutputChannels = outputGate.getNumberOfEdges();
		for (int i = 0; i < numberOfOutputChannels; ++i) {

			final ExecutionEdge c = outputGate.getEdge(i);

			final ExecutionVertex targetVertex = c.getInputGate().getVertex();

			if (targetVertex.getExecutionState() == ExecutionState.ASSIGNED) {
				if (verticesToDeploy == null) {
					verticesToDeploy = new ArrayList<ExecutionVertex>();
				}
				verticesToDeploy.add(targetVertex);
			} else {

				if (targetVertex.getExecutionState() != ExecutionState.RUNNING
					&& targetVertex.getExecutionState() != ExecutionState.FINISHING) {
					return false;
				}
			}
		}

		if (verticesToDeploy != null) {
			this.scheduler.deployAssignedVertices(verticesToDeploy);
			return false;
		}

		return true;
	}

	private static List<AbstractInstance> getClosestReceiverInstances(final AbstractInstance currentRequester,
			final Set<AbstractInstance> remainingReceiverInstances) {

		if (remainingReceiverInstances.isEmpty()) {
			return Collections.emptyList();
		}

		final List<AbstractInstance> closestReceivers = new ArrayList<AbstractInstance>(
			remainingReceiverInstances.size());
		int minimumDistance = Integer.MAX_VALUE;
		Iterator<AbstractInstance> it = remainingReceiverInstances.iterator();
		while (it.hasNext()) {

			final int distance = currentRequester.getDistance(it.next());
			if (distance < minimumDistance) {
				minimumDistance = distance;
			}
		}

		it = remainingReceiverInstances.iterator();
		while (it.hasNext()) {

			final AbstractInstance instance = it.next();
			if (currentRequester.getDistance(instance) == minimumDistance) {
				closestReceivers.add(instance);
			}
		}

		return closestReceivers;
	}

	private static MulticastForwardingTable createForwardingTable(final ExecutionEdge outputChannel) {

		final Map<InstanceConnectionInfo, ConnectionInfoLookupResponse> forwardingTable = new HashMap<InstanceConnectionInfo, ConnectionInfoLookupResponse>();
		final ExecutionGate outputGate = outputChannel.getOutputGate();
		final List<ExecutionEdge> remainingReceivers = new ArrayList<ExecutionEdge>(outputGate.getNumberOfEdges());
		final Queue<AbstractInstance> requesters = new LinkedList<AbstractInstance>();
		final Set<AbstractInstance> remainingReceiverInstances = new HashSet<AbstractInstance>();

		// Initialize data structures
		final int numberOfOutputChannels = outputGate.getNumberOfEdges();
		for (int i = 0; i < numberOfOutputChannels; ++i) {
			final ExecutionEdge edge = outputGate.getEdge(i);
			final ExecutionVertex targetVertex = edge.getInputGate().getVertex();
			remainingReceivers.add(edge);
			remainingReceiverInstances.add(targetVertex.getAllocatedResource().getInstance());
		}
		final ExecutionVertex sourceVertex = outputChannel.getOutputGate().getVertex();
		requesters.add(sourceVertex.getAllocatedResource().getInstance());

		if (LOG.isInfoEnabled()) {
			LOG.info("Creating new multicast forwarding table for " + sourceVertex + ", output channel ID: "
				+ outputChannel.getOutputChannelID());
		}

		while (!requesters.isEmpty()) {

			final AbstractInstance currentRequester = requesters.poll();
			final Iterator<ExecutionEdge> it = remainingReceivers.iterator();
			final List<ChannelID> localTargets = new ArrayList<ChannelID>(16);
			// Find local targets
			while (it.hasNext()) {

				final ExecutionEdge edge = it.next();
				final ExecutionVertex targetVertex = edge.getInputGate().getVertex();
				if (targetVertex.getAllocatedResource().getInstance().equals(currentRequester)) {
					localTargets.add(edge.getInputChannelID());
					it.remove();
				}
			}
			remainingReceiverInstances.remove(currentRequester);

			final List<AbstractInstance> closestReceiverInstances = getClosestReceiverInstances(currentRequester,
				remainingReceiverInstances);
			requesters.addAll(closestReceiverInstances);

			// Construct lookup connection response and add it to the multicast forwarding fable
			final ConnectionInfoLookupResponse cilr = ConnectionInfoLookupResponse.createReceiverFoundAndReady();
			final Iterator<ChannelID> localIt = localTargets.iterator();
			while (localIt.hasNext()) {
				cilr.addLocalTarget(localIt.next());
			}
			final Iterator<AbstractInstance> remoteIt = closestReceiverInstances.iterator();
			while (remoteIt.hasNext()) {
				final InstanceConnectionInfo ici = remoteIt.next().getInstanceConnectionInfo();
				cilr.addRemoteTarget(new RemoteReceiver(new InetSocketAddress(ici.getAddress(), ici.getDataPort()),
					outputChannel.getConnectionID()));
			}

			forwardingTable.put(currentRequester.getInstanceConnectionInfo(), cilr);
		}

		if (!remainingReceivers.isEmpty()) {
			throw new IllegalStateException("There are still " + remainingReceivers.size()
				+ " unprocessed receivers left");
		}

		return new MulticastForwardingTable(forwardingTable);
	}
}