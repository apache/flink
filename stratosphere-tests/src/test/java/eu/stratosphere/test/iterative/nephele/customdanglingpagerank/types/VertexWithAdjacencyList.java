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
package eu.stratosphere.test.iterative.nephele.customdanglingpagerank.types;


/**
 *
 */
public final class VertexWithAdjacencyList {
	
	private static final long[] EMPTY = new long[0];
	
	private long vertexID;
	
	private long[] targets;
	
	private int numTargets;
	
	public VertexWithAdjacencyList() {
		this.targets = EMPTY;
	}
	
	public VertexWithAdjacencyList(long vertexID, long[] targets) {
		this.vertexID = vertexID;
		this.targets = targets;
	}

	
	public long getVertexID() {
		return vertexID;
	}
	
	public void setVertexID(long vertexID) {
		this.vertexID = vertexID;
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
	

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder(32);
		bld.append(this.vertexID);
		bld.append(" : ");
		for (int i = 0; i < this.numTargets; i++) {
			if (i != 0) {
				bld.append(',');
			}
			bld.append(this.targets[i]);
		}
		return bld.toString();
	}
}
