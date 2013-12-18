/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.multicast;

import java.net.InetSocketAddress;
import java.util.LinkedList;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.taskmanager.bytebuffered.RemoteReceiver;

/**
 * Each physical node (instance) within a multicast tree is represented by a TreeNode object.
 * It contains the connection info for the certain node and a list of the local output channels.
 * 
 * @author casp
 */

public class TreeNode implements Comparable<TreeNode> {

	private TreeNode parentnode = null;

	private final AbstractInstance instance;

	private final InstanceConnectionInfo nodeConnectionInfo;

	private final int connectionID;

	private final LinkedList<ChannelID> localTargets;

	private final LinkedList<TreeNode> children = new LinkedList<TreeNode>();

	private final LinkedList<IntegerProperty> properties = new LinkedList<TreeNode.IntegerProperty>();

	private int penalty = 0;

	public TreeNode(AbstractInstance instance, InstanceConnectionInfo nodeConnectionInfo, int connectionID,
			LinkedList<ChannelID> localTargets) {
		this.instance = instance;
		this.connectionID = connectionID;
		this.nodeConnectionInfo = nodeConnectionInfo;
		this.localTargets = localTargets;
	}

	public void setProperty(String key, int value) {
		boolean exists = false;
		for (IntegerProperty property : this.properties) {
			if (property.getKey().equals(key)) {
				property.setValue(value);
				exists = true;
				break;
			}
		}
		if (!exists) {
			this.properties.add(new IntegerProperty(key, value));
		}
	}

	public int getProperty(String key) {
		for (IntegerProperty property : this.properties) {
			if (property.getKey().equals(key)) {
				return property.getValue();
			}
		}
		return -1;
	}

	public void removeChild(TreeNode child) {
		if (this.children.contains(child)) {
			child.setParent(null);
			this.children.remove(child);
		}
	}

	public void addChild(TreeNode child) {
		this.children.add(child);
		child.setParent(this);
	}

	public LinkedList<TreeNode> getChildren() {
		return this.children;
	}

	public TreeNode getParent() {
		return this.parentnode;
	}

	private InstanceConnectionInfo getConnectionInfo() {
		return this.nodeConnectionInfo;
	}

	private int getConnectionID() {
		return this.connectionID;
	}

	private void setParent(TreeNode parent) {
		this.parentnode = parent;
	}

	@Override
	public int compareTo(TreeNode o) {
		return this.nodeConnectionInfo.compareTo(o.nodeConnectionInfo);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TreeNode) {
			return this.nodeConnectionInfo.equals(((TreeNode) o).nodeConnectionInfo);
		} else {
			return false;
		}
	}

	public int getDistance(TreeNode o) {
		return this.instance.getDistance(o.instance);
	}

	public String toString() {
		return this.nodeConnectionInfo.toString();
	}

	public int getPenalty() {
		return this.penalty;
	}

	public void setPenalty(int penalty) {
		this.penalty = penalty;
	}

	/**
	 * This method should be called on the root node (sender node).
	 * It traverses the Tree and returns a full forwarding table
	 * including all local and remote receivers.
	 * 
	 * @return
	 */
	public MulticastForwardingTable createForwardingTable() {
		MulticastForwardingTable table = new MulticastForwardingTable();
		this.generateRecursiveForwardingTable(table);
		return table;
	}

	/**
	 * Private recursive method to generate forwarding table
	 * 
	 * @param table
	 */
	private void generateRecursiveForwardingTable(MulticastForwardingTable table) {

		final ConnectionInfoLookupResponse lookupResponse = ConnectionInfoLookupResponse.createReceiverFoundAndReady();

		// add local targets
		for (final ChannelID i : this.localTargets) {
			lookupResponse.addLocalTarget(i);
		}

		// add remote targets
		for (final TreeNode n : this.children) {

			// Instance Connection info associated with the remote target
			final InstanceConnectionInfo ici = n.getConnectionInfo();

			// get the connection ID associated with the remote target endpoint
			final int icid = n.getConnectionID();

			final InetSocketAddress isa = new InetSocketAddress(ici.getAddress(), ici.getDataPort());

			lookupResponse.addRemoteTarget(new RemoteReceiver(isa, icid));
		}

		table.addConnectionInfo(this.nodeConnectionInfo, lookupResponse);

		for (final TreeNode n : this.children) {
			n.generateRecursiveForwardingTable(table);
		}
	}

	/**
	 * Prints the tree in a human readable format, starting with the actual node as root.
	 * 
	 * @return
	 */
	public String printTree() {

		StringBuilder sb = new StringBuilder();
		this.printRecursiveTree(sb);
		return sb.toString();
	}

	private void printRecursiveTree(StringBuilder sb) {

		if (this.children.size() > 0) {
			sb.append("STRUCT ");

			sb.append(this.nodeConnectionInfo);

			for (TreeNode n : this.children) {
				sb.append(" ");
				sb.append(n.getConnectionInfo().toString());
			}

			sb.append("\n");

			for (TreeNode n : this.children) {
				n.printRecursiveTree(sb);
			}
		}
	}

	private static class IntegerProperty {

		private String key = null;

		private int value = 0;

		public IntegerProperty(final String key, final int value) {
			this.key = key;
			this.value = value;
		}

		public String getKey() {
			return this.key;
		}

		public int getValue() {
			return this.value;
		}

		public void setValue(final int value) {
			this.value = value;
		}
	}

}
