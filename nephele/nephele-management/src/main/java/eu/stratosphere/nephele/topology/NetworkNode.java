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

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.types.StringRecord;

public class NetworkNode implements IOReadableWritable {

	private final NetworkTopology networkTopology;

	private String name = null;

	private final NetworkNode parentNode;

	private final List<NetworkNode> childNodes = new ArrayList<NetworkNode>();

	private Object attachment;

	protected NetworkNode(String name, NetworkNode parentNode, NetworkTopology networkTopology) {
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

	NetworkNode(NetworkNode parentNode, NetworkTopology networkTopology) {
		this.parentNode = parentNode;
		this.networkTopology = networkTopology;

		// The node will add itself when it is fully deserialized
	}

	private void addChild(NetworkNode child) {

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

	private void removeChild(NetworkNode child) {
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

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}

	public NetworkNode getChildNode(int index) {

		if (index < this.childNodes.size()) {
			return this.childNodes.get(index);
		}

		return null;
	}

	public NetworkNode getParentNode() {
		return this.parentNode;
	}

	public NetworkTopology getNetworkTopology() {
		return this.networkTopology;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

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
	public void write(DataOutput out) throws IOException {

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
			str = "[";
			final Iterator<NetworkNode> it = this.childNodes.iterator();
			while (it.hasNext()) {
				str += it.next().toString();
				if (it.hasNext()) {
					str += ", ";
				}
			}
			str += "]";
		}

		return str;
	}
}