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

package eu.stratosphere.nephele.multicast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.scheduler.Scheduler;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.types.Record;

/**
 * The MulticastManager is responsible for the creation and storage of application-layer multicast trees used to
 * broadcast records to multiple target vertices.
 * 
 * @author casp
 */

public class MulticastManager implements ChannelLookupProtocol {

	private final Scheduler scheduler;

	private final Map<ChannelID, MulticastForwardingTable> cachedTrees = new HashMap<ChannelID, MulticastForwardingTable>();

	private final TopologyInformationSupplier topologySupplier = new TopologyInformationSupplier();

	public MulticastManager(Scheduler scheduler) {
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
	public synchronized ConnectionInfoLookupResponse lookupConnectionInfo(InstanceConnectionInfo caller, JobID jobID,
			ChannelID sourceChannelID) {

		System.out.println("==RECEIVING REQUEST FROM " + caller + " == SOURCE CHANNEL:  " + sourceChannelID);
		// check, if the tree is already created and cached
		if (this.cachedTrees.containsKey(sourceChannelID)) {
			System.out.println("==RETURNING CACHED ENTRY TO " + caller + " ==");
			System.out.println(cachedTrees.get(sourceChannelID).getConnectionInfo(caller));
			System.out.println("==END ENTRY==");

			return cachedTrees.get(sourceChannelID).getConnectionInfo(caller);
		} else {

			// no tree exists - we assume that this is the sending node initiating a multicast

			// first check, if all receivers are up and ready
			if (!checkIfAllTargetVerticesExist(caller, jobID, sourceChannelID)) {
				// not all target vertices exist..
				System.out.println("== NOT ALL RECEIVERS FOUND==");
				return ConnectionInfoLookupResponse.createReceiverNotFound();
			}

			if (!checkIfAllTargetVerticesReady(caller, jobID, sourceChannelID)) {
				// not all target vertices are ready..
				System.out.println("== NOT ALL RECEIVERS READY==");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			// receivers up and running.. create tree
			LinkedList<TreeNode> treenodes = extractTreeNodes(caller, jobID, sourceChannelID);
			
			
			cachedTrees.put(sourceChannelID, MulticastCluster.createClusteredTree(treenodes, 2));
			
			//cachedTrees.put(sourceChannelID, createSequentialTree(treenodes));

			System.out.println("==RETURNING ENTRY TO " + caller + " ==");
			System.out.println(cachedTrees.get(sourceChannelID).getConnectionInfo(caller));
			System.out.println("==END ENTRY==");
			return cachedTrees.get(sourceChannelID).getConnectionInfo(caller);

		}

	}

	/**
	 * Creates a simple sequential multicast tree out of a list of tree nodes.
	 * Each node forwards to local targets as well as to the next physical instance in the list.
	 * 
	 * @param nodes
	 * @return
	 */
	private MulticastForwardingTable createSequentialTree(LinkedList<TreeNode> nodes) {
		MulticastForwardingTable table = new MulticastForwardingTable();

		System.out.println("+++TREE+++");
		while (nodes.size() > 0) {
			TreeNode actualnode = nodes.pollFirst();

			System.out.println("POLL FIRST: " + actualnode.getConnectionInfo());

			ConnectionInfoLookupResponse actualentry = ConnectionInfoLookupResponse.createReceiverFoundAndReady();

			// add all local targets
			for (ChannelID id : actualnode.getLocalTargets()) {
				System.out.println("local target: " + id);
				actualentry.addLocalTarget(id);
			}

			// add remote target - next node in the list
			if (nodes.size() > 0) {
				System.out.println("remote target: " + nodes.getFirst().getConnectionInfo());
				actualentry.addRemoteTarget(nodes.getFirst().getConnectionInfo());
			}

			table.addConnectionInfo(actualnode.getConnectionInfo(), actualentry);
		}
		System.out.println("+++END TREE+++");
		return table;
	}

	private MulticastForwardingTable createClusteredTree(LinkedList<TreeNode> nodes) {

		return null;

	}

	/**
	 * Checks, if all target vertices for a multicast transmission exist.
	 * 
	 * @param caller
	 * @param jobID
	 * @param sourceChannelID
	 * @return
	 */
	private boolean checkIfAllTargetVerticesExist(InstanceConnectionInfo caller, JobID jobID, ChannelID sourceChannelID) {
		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);

		final AbstractOutputChannel<? extends Record> outputChannel = eg.getOutputChannelByID(sourceChannelID);

		final OutputGate<? extends Record> broadcastgate = outputChannel.getOutputGate();

		// get all broadcast output channels
		for (AbstractOutputChannel<? extends Record> c : broadcastgate.getOutputChannels()) {
			if (c.isBroadcastChannel()) {
				ExecutionVertex targetVertex = eg.getVertexByChannelID(c.getConnectedChannelID());
				if (targetVertex == null) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Checks, if all target vertices for multicast transmisison are ready.
	 * 
	 * @param caller
	 * @param jobID
	 * @param sourceChannelID
	 * @return
	 */
	private boolean checkIfAllTargetVerticesReady(InstanceConnectionInfo caller, JobID jobID, ChannelID sourceChannelID) {
		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);

		final AbstractOutputChannel<? extends Record> outputChannel = eg.getOutputChannelByID(sourceChannelID);

		final OutputGate<? extends Record> broadcastgate = outputChannel.getOutputGate();

		// get all broadcast output channels
		for (AbstractOutputChannel<? extends Record> c : broadcastgate.getOutputChannels()) {
			if (c.isBroadcastChannel()) {
				ExecutionVertex targetVertex = eg.getVertexByChannelID(c.getConnectedChannelID());
				if (targetVertex.getExecutionState() != ExecutionState.RUNNING
					&& targetVertex.getExecutionState() != ExecutionState.FINISHING) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Returns a list of (physical) Nodes (=hosts) within the multicast tree. Each node contains the local ChannelIDs,
	 * records
	 * must be forwarded to. The first node in the List is the only multicast sender.
	 * 
	 * @param sourceChannelID
	 * @return
	 */
	private LinkedList<TreeNode> extractTreeNodes(InstanceConnectionInfo source, JobID jobID, ChannelID sourceChannelID) {
		System.out.println("==NO CACHE ENTRY FOUND. CREATING TREE==");
		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);

		final AbstractOutputChannel<? extends Record> outputChannel = eg.getOutputChannelByID(sourceChannelID);

		final OutputGate<? extends Record> broadcastgate = outputChannel.getOutputGate();

		System.out.println("Output gate is: " + broadcastgate.toString());

		final LinkedList<AbstractOutputChannel<? extends Record>> outputChannels = new LinkedList<AbstractOutputChannel<? extends Record>>();

		// get all broadcast output channels
		for (AbstractOutputChannel<? extends Record> c : broadcastgate.getOutputChannels()) {
			if (c.isBroadcastChannel()) {
				outputChannels.add(c);
			}
		}

		System.out.println("Number of output channels attached: " + outputChannels.size());

		for (AbstractOutputChannel<? extends Record> c : broadcastgate.getOutputChannels()) {
			System.out.println("Out channel ID: "
				+ c.getID()
				+ " connected channel: "
				+ c.getConnectedChannelID()
				+ " target instance: "
				+ eg.getVertexByChannelID(c.getConnectedChannelID()).getAllocatedResource().getInstance()
					.getInstanceConnectionInfo());
		}

		final LinkedList<TreeNode> treenodes = new LinkedList<TreeNode>();

		TreeNode actualnode;

		// create sender node (root) with source instance
		actualnode = new TreeNode(source);

		// search for local targets for the tree node
		for (Iterator<AbstractOutputChannel<? extends Record>> iter = outputChannels.iterator(); iter.hasNext();) {

			AbstractOutputChannel<? extends Record> actualoutputchannel = iter.next();

			ExecutionVertex targetVertex = eg.getVertexByChannelID(actualoutputchannel.getConnectedChannelID());

			// is the target vertex running on the same instance?
			if (targetVertex.getAllocatedResource().getInstance().getInstanceConnectionInfo().equals(source)) {
				actualnode.addLocalTarget(actualoutputchannel.getConnectedChannelID());

				iter.remove();

			}

		}

		treenodes.add(actualnode);

		// now we have the root-node.. lets extract all other nodes

		while (outputChannels.size() > 0) {

			AbstractOutputChannel<? extends Record> firstChannel = outputChannels.pollFirst();

			ExecutionVertex firstTarget = eg.getVertexByChannelID(firstChannel.getConnectedChannelID());

			InstanceConnectionInfo actualinstance = firstTarget.getAllocatedResource().getInstance()
				.getInstanceConnectionInfo();

			// create tree node for current instance
			actualnode = new TreeNode(actualinstance);

			// add first local target
			actualnode.addLocalTarget(firstChannel.getConnectedChannelID());

			// now we iterate through the remaining channels to find other local targets...
			for (Iterator<AbstractOutputChannel<? extends Record>> iter = outputChannels.iterator(); iter.hasNext();) {
				AbstractOutputChannel<? extends Record> actualoutputchannel = iter.next();

				ExecutionVertex actualTarget = eg.getVertexByChannelID(actualoutputchannel.getConnectedChannelID());

				// is the target vertex running on the same instance?
				if (actualTarget.getAllocatedResource().getInstance().getInstanceConnectionInfo()
					.equals(actualinstance)) {
					actualnode.addLocalTarget(actualoutputchannel.getConnectedChannelID());

					iter.remove();

				}

			}// end for

			treenodes.add(actualnode);

		}// end while

		return treenodes;

	}

}