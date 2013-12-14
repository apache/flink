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

package eu.stratosphere.nephele.topology;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;

public class NetworkNode implements IOReadableWritable {

	private final NetworkTopology networkTopology;

	private String name = null;

	private final NetworkNode parentNode;

	private final List<NetworkNode> childNodes = new ArrayList<NetworkNode>();

	private Object attachment;

	protected NetworkNode(final String name, final NetworkNode parentNode, final NetworkTopology networkTopology) {
		this.name = name;
		this.parentNode = parentNode;
		this.networkTopology = networkTopology;

		if (this.parentNode != null) {
			this.parentNode.addChild(this);
		}

		if (this.networkTopology != null) {
			this.networkTopology.addNode(this);
		}
	}

	NetworkNode(final NetworkNode parentNode, final NetworkTopology networkTopology) {
		this.parentNode = parentNode;
		this.networkTopology = networkTopology;

		// The node will add itself when it is fully deserialized
	}

	private void addChild(final NetworkNode child) {

		this.childNodes.add(child);
	}

	public void remove() {

		if (!isLeafNode()) {
			return;
		}

		if (this.parentNode != null) {
			this.parentNode.removeChild(this);
		}

		if (this.networkTopology != null) {
			this.networkTopology.removeNode(this);
		}
	}

	private void removeChild(final NetworkNode child) {
		this.childNodes.remove(child);
	}

	public boolean isRootNode() {
		return (this.parentNode == null);
	}

	public boolean isLeafNode() {
		return this.childNodes.isEmpty();
	}

	public String getName() {
		return this.name;
	}

	public int getDepth() {

		if (this.isRootNode()) {
			return 1;
		}

		return (1 + this.parentNode.getDepth());
	}

	public int getHeight() {

		int maxHeight = 0;
		final Iterator<NetworkNode> it = this.childNodes.iterator();
		while (it.hasNext()) {
			final int height = it.next().getHeight();
			if (height > maxHeight) {
				maxHeight = height;
			}
		}

		return (1 + maxHeight);
	}

	public int getNumberOfChildNodes() {
		return this.childNodes.size();
	}

	public void setAttachment(final Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}

	public NetworkNode getChildNode(final int index) {

		if (index < this.childNodes.size()) {
			return this.childNodes.get(index);
		}

		return null;
	}

	public NetworkNode getParentNode() {
		return this.parentNode;
	}

	/**
	 * Returns the network topology that is associated with this network node.
	 * 
	 * @return the network topology that is associated with this network node
	 */
	public NetworkTopology getNetworkTopology() {
		return this.networkTopology;
	}

	/**
	 * Determines the distance to the given network node. The distance is determined as the number of internal network
	 * nodes that must be traversed in order to send a packet from one node to the other plus one.
	 * 
	 * @param networkNode
	 *        the node to determine the distance for
	 * @return the distance to the given network node or <code>Integer.MAX_VALUE</code> if the given node is not part of
	 *         this node's network topology
	 */
	public int getDistance(final NetworkNode networkNode) {

		int steps = 0;
		NetworkNode tmp = this;
		while (tmp != null) {

			final int distance = tmp.isPredecessorOrSelfOf(networkNode);
			if (distance >= 0) {
				return (steps + distance);
			}

			tmp = tmp.getParentNode();
			++steps;
		}

		return Integer.MAX_VALUE;
	}

	/**
	 * Checks whether this node is a predecessor or the identity (the node itself) of the given network node in the
	 * network topology tree.
	 * 
	 * @param networkNode
	 *        a potential child network node
	 * @return If this node node is a predecessor of given node in the network topology tree, the return value
	 *         indicates the distance between both nodes. If the given node equals this node, the
	 *         return value is <code>0</code>. Otherwise the return value is <code>-1</code>.
	 */
	private int isPredecessorOrSelfOf(final NetworkNode networkNode) {

		NetworkNode tmp = networkNode;
		int steps = 0;
		while (tmp != null) {

			if (this.equals(tmp)) {
				return steps;
			}

			tmp = tmp.getParentNode();
			++steps;
		}

		return -1;
	}

	public int getDistance(final String nodeName) {

		final NetworkNode networkNode = this.networkTopology.getNodeByName(nodeName);
		if (networkNode == null) {
			return Integer.MAX_VALUE;
		}

		if (this.equals(networkNode)) {
			return 0;
		}

		return getDistance(networkNode);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.name = StringRecord.readString(in);

		// We need to read the name before we can add the node to the topology's node map
		if (this.networkTopology != null) {
			this.networkTopology.addNode(this);
		}

		final int numberOfChildNodes = in.readInt();
		for (int i = 0; i < numberOfChildNodes; i++) {
			final NetworkNode networkNode = new NetworkNode(this, this.networkTopology);
			networkNode.read(in);
			this.childNodes.add(networkNode);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		StringRecord.writeString(out, this.name);
		out.writeInt(this.childNodes.size());
		final Iterator<NetworkNode> it = this.childNodes.iterator();
		while (it.hasNext()) {
			it.next().write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		String str;
		if (this.childNodes.isEmpty()) {
			str = this.name;
		} else {
			final Iterator<NetworkNode> it = this.childNodes.iterator();
			final StringBuffer buf = new StringBuffer("[");
			while (it.hasNext()) {
				buf.append(it.next().toString());
				if (it.hasNext()) {
					buf.append(", ");
				}
			}

			buf.append("]");
			str = buf.toString();
		}

		return str;
	}
}
