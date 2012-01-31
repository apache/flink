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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;

/**
 * This class represents a cluster of hosts within a multicast-tree.
 * 
 * @author casp
 */

public class MulticastCluster {
	TreeNode master = null;

	HashSet<TreeNode> clusternodes = new HashSet<TreeNode>();

	public void addNode(TreeNode node) {
		this.clusternodes.add(node);
	}

	public int getSize() {
		return this.clusternodes.size();
	}

	public HashSet<TreeNode> getNodes() {
		return this.clusternodes;
	}

	/**
	 * Returns the master-node of the current cluster.
	 * 
	 * @return
	 */
	public TreeNode getMaster() {

		if (this.master == null) {

			// TODO: topology-aware!!
			if (clusternodes.size() != 0) {
				this.master = clusternodes.iterator().next();
			} else {
				System.out.println("cluster is empty.");
				return null;
			}

		}

		return this.master;
	}

	/**
	 * Splits the cluster into an arbitrary number of clusters not exceeding maxsize.
	 * 
	 * @param maxsize
	 * @return
	 */
	public HashSet<MulticastCluster> splitCluster(int maxsize) {
		// TODO: topology-aware!
		HashSet<MulticastCluster> newClusters = new HashSet<MulticastCluster>();

		MulticastCluster actualcluster = new MulticastCluster();

		for (Iterator<TreeNode> i = this.clusternodes.iterator(); i.hasNext();) {
			if (actualcluster.getSize() < maxsize) {
				actualcluster.addNode(i.next());
			} else {
				// cluster is full.. add old cluster to list
				newClusters.add(actualcluster);
				// and create new cluster object
				actualcluster = new MulticastCluster();
				actualcluster.addNode(i.next());
			}
		}

		newClusters.add(actualcluster);

		return newClusters;

	}

	public static MulticastCluster createInitialCluster(Collection<TreeNode> nodes) {
		// TODO: topology-aware? in here?

		MulticastCluster cluster = new MulticastCluster();

		for (TreeNode n : nodes) {
			cluster.addNode(n);
		}

		return cluster;

	}

	public static MulticastForwardingTable createClusteredTree(LinkedList<TreeNode> nodes, int maxclustersize) {

		return null;
		/*
		// List to store all levels of the clustered multicast tree
		LinkedList<HashSet<MulticastCluster>> clusterlist = new LinkedList<HashSet<MulticastCluster>>();

		// Poll off the sending node first..
		TreeNode source = nodes.pollFirst();

		// Create an initital multicast cluster containing all receivers
		MulticastCluster initialcluster = createInitialCluster(nodes);

		// Create a first layer of clusters with arbitrary size by splitting the initital cluster
		HashSet<MulticastCluster> firstlaycluster = initialcluster.splitCluster(maxclustersize);

		// add to the list of cluster layers
		clusterlist.add(firstlaycluster);

		// we want the top layer to consist of max. maxclustersize clusters
		while (clusterlist.getFirst().size() > maxclustersize) {

			// creating a new cluster-layer...
			MulticastCluster nextlayercluster = new MulticastCluster();

			HashSet<MulticastCluster> lowerlayer = clusterlist.getFirst();

			// add all master nodes from current layer to next-layer cluster...
			for (MulticastCluster c : lowerlayer) {
				nextlayercluster.addNode(c.getMaster());
			}

			// if our next-layer cluster is still too big, we need to split it again
			HashSet<MulticastCluster> nextlayerclusters = nextlayercluster.splitCluster(maxclustersize);

			// and finally ad the new layer of clusters
			clusterlist.addFirst(nextlayerclusters);

		}

		// now we can create the tree...

		MulticastForwardingTable table = new MulticastForwardingTable();

		HashSet<MulticastCluster> initialclusterlevel = clusterlist.getFirst();

		ConnectionInfoLookupResponse sourceentry = ConnectionInfoLookupResponse.createReceiverFoundAndReady();

		// add all local targets
		for (ChannelID id : source.getLocalTargets()) {
			System.out.println("local target: " + id);
			sourceentry.addLocalTarget(id);
		}

		// connect source node with all master nodes in top-level clusters
		for (MulticastCluster c : initialclusterlevel) {
			sourceentry.addRemoteTarget(c.getMaster().getConnectionInfo());
		}

		table.addConnectionInfo(source.getConnectionInfo(), sourceentry);
		System.out.println("forwards for node: " + source.getConnectionInfo());
		System.out.println(sourceentry);
		// now we have connected the source node to the initial cluster layer. iterate through cluster layers and
		// connect

		while (clusterlist.size() > 0) {
			HashSet<MulticastCluster> actualclusterlevel = clusterlist.pollFirst();

			// add remote targets!

			for (MulticastCluster c : actualclusterlevel) {
				TreeNode master = c.getMaster();
				for (Iterator<TreeNode> i = c.getNodes().iterator(); i.hasNext();) {
					TreeNode actualnode = i.next();
					if (!actualnode.equals(master)) {
						// add remote target at master of current cluster
						master.addRemoteTarget(actualnode.getConnectionInfo());
					}
				}
			}

		}

		// now iterate through all nodes and create forwarding table...
		// we already have the entry for the source node..
		for (TreeNode n : nodes) {
			ConnectionInfoLookupResponse actualentry = ConnectionInfoLookupResponse.createReceiverFoundAndReady();
			for (ChannelID localTarget : n.getLocalTargets()) {
				actualentry.addLocalTarget(localTarget);
			}
			for (InstanceConnectionInfo remoteTarget : n.getRemoteTargets()) {
				actualentry.addRemoteTarget(remoteTarget);
			}
			table.addConnectionInfo(n.getConnectionInfo(), actualentry);
			System.out.println("forwards for node: " + n.getConnectionInfo());
			System.out.println(actualentry);
		}

		return table;
		*/
	}

}
