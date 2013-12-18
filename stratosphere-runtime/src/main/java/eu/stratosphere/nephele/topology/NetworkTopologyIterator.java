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

package eu.stratosphere.nephele.topology;

import java.util.Iterator;
import java.util.Stack;

public class NetworkTopologyIterator implements Iterator<NetworkNode> {

	private static class TraversalEntry {

		private NetworkNode networkNode;

		private int childrenVisited = 0;

		public TraversalEntry(final NetworkNode networkNode) {
			this.networkNode = networkNode;
		}

		public NetworkNode getNetworkNode() {
			return this.networkNode;
		}

		public int getChildrenVisited() {
			return this.childrenVisited;
		}

		public void increaseChildrenVisited() {
			++this.childrenVisited;
		}
	}

	private Stack<TraversalEntry> traversalStack = new Stack<TraversalEntry>();

	NetworkTopologyIterator(final NetworkTopology networkTopology) {

		traversalStack.add(new TraversalEntry(networkTopology.getRootNode()));
		refillStack();
	}

	private void refillStack() {

		while (true) {

			final TraversalEntry traversalEntry = this.traversalStack.peek();
			final NetworkNode networkNode = traversalEntry.getNetworkNode();
			if (networkNode.isLeafNode()) {
				break;
			}

			final NetworkNode childNode = networkNode.getChildNode(traversalEntry.getChildrenVisited());
			this.traversalStack.add(new TraversalEntry(childNode));
		}

	}


	@Override
	public boolean hasNext() {

		if (this.traversalStack.isEmpty()) {
			return false;
		}

		return true;
	}


	@Override
	public NetworkNode next() {

		final TraversalEntry traversalEntry = this.traversalStack.pop();
		final NetworkNode networkNode = traversalEntry.networkNode;

		if (!this.traversalStack.isEmpty()) {
			final TraversalEntry parentTraversalEntry = this.traversalStack.peek();
			parentTraversalEntry.increaseChildrenVisited();
			if (parentTraversalEntry.getChildrenVisited() < parentTraversalEntry.getNetworkNode()
				.getNumberOfChildNodes()) {
				refillStack();
			}
		}

		return networkNode;
	}


	@Override
	public void remove() {
		// Optional operation

	}

}
