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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;

/**
 * The MulticastManager is responsible for the creation and storage of application-layer multicast trees used to
 * broadcast records to multiple target vertices.
 * 
 * @author casp
 */

public class MulticastManager implements ChannelLookupProtocol {

	private static final Log LOG = LogFactory.getLog(JobManager.class);

	// Indicates if the arrangement of nodes within the overlay-tree should be randomized or not.
	// If set to false, arrangement of the same set of receiver nodes is guaranteed to be the same
	private final boolean randomized;

	// Indicates if the tree should be constructed with a given topology stored in a file
	private final boolean usehardcodedtree;

	// File containing the hard-coded tree topology, if desired
	// Should contain node names (eg hostnames) with corresponding children per line
	// eg: a line "vm1.local vm2.local vm3.local"
	// would result in vm1.local connecting to vm2.local and vm3.local as children
	// no further checking for connectivity of the given topology is done!
	private final String hardcodedtreefilepath;

	// Indicates the desired branching of the generated multicast-tree. 0 means unicast transmisison, 1 sequential tree
	// 2 binomial tree, 3+ clustered tree
	private final int treebranching;

	private final AbstractScheduler scheduler;

	private final Map<ChannelID, MulticastForwardingTable> cachedTrees = new HashMap<ChannelID, MulticastForwardingTable>();

	public MulticastManager(final AbstractScheduler scheduler) {
		this.scheduler = scheduler;

		this.randomized = GlobalConfiguration.getBoolean("multicast.randomize", false);
		this.treebranching = GlobalConfiguration.getInteger("multicast.branching", 1);
		this.usehardcodedtree = GlobalConfiguration.getBoolean("multicast.usehardcodedtree", false);
		this.hardcodedtreefilepath = GlobalConfiguration.getString("multicast.hardcodedtreefile", null);
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

		LOG.info("Receiving multicast receiver request from " + caller + " channel ID: " + sourceChannelID);

		// check, if the tree is already created and cached
		if (this.cachedTrees.containsKey(sourceChannelID)) {
			LOG.info("Replying with cached entry...");
			return cachedTrees.get(sourceChannelID).getConnectionInfo(caller);
		} else {

			// no tree exists - we assume that this is the sending node initiating a multicast

			if (!checkIfAllTargetVerticesReady(caller, jobID, sourceChannelID)) {
				LOG.info("Received multicast request but not all receivers ready.");

				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			// receivers up and running.. extract tree nodes...
			LinkedList<TreeNode> treenodes = extractTreeNodes(caller, jobID, sourceChannelID, this.randomized);

			// Do we want to use a hard-coded tree topology?
			if (this.usehardcodedtree) {
				LOG.info("Creating a hard-coded tree topology from file: " + hardcodedtreefilepath);
				cachedTrees.put(sourceChannelID, createHardCodedTree(treenodes));
				return cachedTrees.get(sourceChannelID).getConnectionInfo(caller);
			}

			// Otherwise we create a default tree and put it into the tree-cache
			cachedTrees.put(sourceChannelID, createDefaultTree(treenodes, this.treebranching));
			return cachedTrees.get(sourceChannelID).getConnectionInfo(caller);

		}

	}

	/**
	 * Returns and removes the TreeNode which is closest to the given indicator.
	 * 
	 * @param indicator
	 * @param nodes
	 * @return
	 */
	private TreeNode pollClosestNode(TreeNode indicator, LinkedList<TreeNode> nodes) {

		TreeNode closestnode = getClosestNode(indicator, nodes);

		nodes.remove(closestnode);

		return closestnode;

	}

	/**
	 * Returns the TreeNode which is closest to the given indicator Node. Proximity is determined
	 * either using topology-information (if given), penalty information (if given) or it returns
	 * the first node in the list.
	 * 
	 * @param indicator
	 * @param nodes
	 * @return
	 */
	private TreeNode getClosestNode(TreeNode indicator, LinkedList<TreeNode> nodes) {

		if (indicator == null) {
			return nodes.getFirst();
		}

		TreeNode closestnode = null;
		for (TreeNode n : nodes) {
			if (closestnode == null || n.getDistance(indicator) < closestnode.getDistance(indicator)) {
				closestnode = n;
			}
		}

		return closestnode;
	}

	/**
	 * This method creates a tree with an arbitrary fan out (two means binary tree).
	 * If topology information or penalties are available, it considers that.
	 * If fanout is set to 1, it creates a sequential tree.
	 * if fanout is set to Integer.MAXVALUE, it creates a unicast tree.
	 * 
	 * @param nodes
	 * @param fanout
	 * @return
	 */
	private MulticastForwardingTable createDefaultTree(LinkedList<TreeNode> nodes, int fanout) {

		// Store nodes that already have a parent, but no children
		LinkedList<TreeNode> connectedNodes = new LinkedList<TreeNode>();

		final TreeNode rootnode = nodes.pollFirst();
		TreeNode actualnode = rootnode;

		while (nodes.size() > 0) { // We still have unconnected nodes...

			for (int i = 0; i < fanout; i++) {

				if (nodes.size() > 0) {
					// pick the closest one and attach to actualnode
					TreeNode child = pollClosestNode(actualnode, nodes);
					actualnode.addChild(child);

					// The child is now connected and can be used as forwarder in the next iteration..
					connectedNodes.add(child);
				} else {
					break;
				}
			}

			// OK.. take the next node to attach children to it..
			// TODO: Optimization? "pollBest()" ?
			actualnode = connectedNodes.pollFirst();

		}
		LOG.info("created multicast tree with following topology:\n" + rootnode.printTree());

		return rootnode.createForwardingTable();

	}

	/**
	 * Reads a hard-coded tree topology from file and creates a tree according to the hard-coded
	 * topology from the file.
	 * 
	 * @param nodes
	 * @return
	 */
	private MulticastForwardingTable createHardCodedTree(LinkedList<TreeNode> nodes) {
		try {
			FileInputStream fstream = new FileInputStream(this.hardcodedtreefilepath);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				String[] values = strLine.split(" ");
				String actualhostname = values[0];
				for (TreeNode n : nodes) {
					if (n.toString().equals(actualhostname)) {
						// we found the node.. connect the children
						for (int i = 1; i < values.length; i++) {
							for (TreeNode childnode : nodes) {
								if (childnode.toString().equals(values[i])) {
									n.addChild(childnode);
								}
							}
						}
					}
				}
			}
			br.close();
			// First node is root.. create tree. easy
			return nodes.getFirst().createForwardingTable();

		} catch (Exception e) {
			System.out.println("Error reading hard-coded topology file for multicast tree: " + e.getMessage());
			return null;
		}
	}

	/**
	 * Checks, if all target vertices for multicast transmisison are ready. If vertices are in state ASSIGNED, it will
	 * deploy those vertices.
	 * 
	 * @param caller
	 * @param jobID
	 * @param sourceChannelID
	 * @return
	 */
	private boolean checkIfAllTargetVerticesReady(InstanceConnectionInfo caller, JobID jobID, ChannelID sourceChannelID) {
		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);

		final ExecutionEdge outputChannel = eg.getEdgeByID(sourceChannelID);

		final ExecutionGate broadcastGate = outputChannel.getOutputGate();

		List<ExecutionVertex> verticesToDeploy = null;

		// get all broadcast output channels
		final int numberOfOutputChannels = broadcastGate.getNumberOfEdges();
		for (int i = 0; i < numberOfOutputChannels; ++i) {

			final ExecutionEdge c = broadcastGate.getEdge(i);

			if (c.isBroadcast()) {

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
		}

		if (verticesToDeploy != null) {
			this.scheduler.deployAssignedVertices(verticesToDeploy);
			return false;
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
	private LinkedList<TreeNode> extractTreeNodes(InstanceConnectionInfo source, JobID jobID,
			ChannelID sourceChannelID, boolean randomize) {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);

		final ExecutionEdge outputChannel = eg.getEdgeByID(sourceChannelID);
		
		final ExecutionGate broadcastGate = outputChannel.getOutputGate();

		final LinkedList<ExecutionEdge> outputChannels = new LinkedList<ExecutionEdge>();

		// Get all broadcast output channels
		final int numberOfOutputChannels = broadcastGate.getNumberOfEdges();
		for (int i = 0; i < numberOfOutputChannels; ++i) {
			final ExecutionEdge c = broadcastGate.getEdge(i);

			if (c.isBroadcast()) {
				outputChannels.add(c);
			}
		}

		final LinkedList<TreeNode> treeNodes = new LinkedList<TreeNode>();

		LinkedList<ChannelID> actualLocalTargets = new LinkedList<ChannelID>();

		int firstConnectionID = 0;
		// search for local targets for the tree node
		for (Iterator<ExecutionEdge> iter = outputChannels.iterator(); iter.hasNext();) {

			final ExecutionEdge actualOutputChannel = iter.next();

			// the connection ID should not be needed for the root node (as it is not set as remote receiver)
			// but in order to maintain consistency, it also gets the connectionID of the first channel pointing to it
			firstConnectionID = actualOutputChannel.getConnectionID();
			
			final ExecutionVertex targetVertex = actualOutputChannel.getInputGate().getVertex();

			// is the target vertex running on the same instance?
			if (targetVertex.getAllocatedResource().getInstance().getInstanceConnectionInfo().equals(source)) {

				actualLocalTargets.add(actualOutputChannel.getInputChannelID());
				iter.remove();
			}

		}

		// create sender node (root) with source instance
		TreeNode actualNode = new TreeNode(eg.getVertexByChannelID(sourceChannelID).getAllocatedResource()
			.getInstance(), source, firstConnectionID, actualLocalTargets);

		treeNodes.add(actualNode);

		// now we have the root-node.. lets extract all other nodes

		LinkedList<TreeNode> receivernodes = new LinkedList<TreeNode>();

		while (outputChannels.size() > 0) {

			final ExecutionEdge firstChannel = outputChannels.pollFirst();
			
			// each receiver nodes' endpoint is associated with the connection ID
			// of the first channel pointing to this node.
			final int connectionID = firstChannel.getConnectionID();

			final ExecutionVertex firstTarget = firstChannel.getInputGate().getVertex();

			final InstanceConnectionInfo actualInstance = firstTarget.getAllocatedResource().getInstance()
				.getInstanceConnectionInfo();

			actualLocalTargets = new LinkedList<ChannelID>();

			// add first local target
			actualLocalTargets.add(firstChannel.getInputChannelID());

			// now we iterate through the remaining channels to find other local targets...
			for (Iterator<ExecutionEdge> iter = outputChannels.iterator(); iter.hasNext();) {

				final ExecutionEdge actualOutputChannel = iter.next();

				final ExecutionVertex actualTarget = actualOutputChannel.getInputGate().getVertex();

				// is the target vertex running on the same instance?
				if (actualTarget.getAllocatedResource().getInstance().getInstanceConnectionInfo()
					.equals(actualInstance)) {
					actualLocalTargets.add(actualOutputChannel.getInputChannelID());
					
					iter.remove();

				}

			}// end for

			// create tree node for current instance
			actualNode = new TreeNode(firstTarget.getAllocatedResource().getInstance(), actualInstance, connectionID,
				actualLocalTargets);
			

			receivernodes.add(actualNode);

		}// end while

		// Do we want to shuffle the receiver nodes?
		// Only randomize the receivers, as the sender (the first one) has to stay the same
		if (randomize) {
			Collections.shuffle(receivernodes);
		} else {
			// Sort Tree Nodes according to host name..
			Collections.sort(receivernodes);
		}

		treeNodes.addAll(receivernodes);

		return treeNodes;

	}

}