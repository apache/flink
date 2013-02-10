/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.types;


/**
 *
 */
public final class NodeWithAdjacencyList {
	
	private static final long[] EMPTY = new long[0];
	
	private long nodeId;
	
	private long[] targets;
	
	private int numTargets;
	
	public NodeWithAdjacencyList() {
		this.targets = EMPTY;
	}
	
	public NodeWithAdjacencyList(long nodeId, long[] targets) {
		this.nodeId = nodeId;
		this.targets = targets;
	}

	
	public long getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(long nodeId) {
		this.nodeId = nodeId;
	}
	
	public long[] getTargets() {
		return targets;
	}
	
	public void setTargets(long[] targets) {
		this.targets = targets;
	}
	
	public int getNumTargets() {
		return numTargets;
	}
	
	public void setNumTargets(int numTargets) {
		this.numTargets = numTargets;
	}
}
