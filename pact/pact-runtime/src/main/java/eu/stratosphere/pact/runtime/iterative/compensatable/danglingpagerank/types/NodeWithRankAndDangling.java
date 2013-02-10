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
public final class NodeWithRankAndDangling {

	private long nodeId;
	
	private double rank;

	private boolean dangling;
	

	public NodeWithRankAndDangling() {
	}
	
	public NodeWithRankAndDangling(long nodeId, double rank, boolean dangling) {
		this.nodeId = nodeId;
		this.rank = rank;
		this.dangling = dangling;
	}

	
	public long getNodeId() {
		return nodeId;
	}
	
	public void setNodeId(long nodeId) {
		this.nodeId = nodeId;
	}
	
	public double getRank() {
		return rank;
	}
	
	public void setRank(double rank) {
		this.rank = rank;
	}
	
	public boolean isDangling() {
		return dangling;
	}
	
	public void setDangling(boolean dangling) {
		this.dangling = dangling;
	}
}
