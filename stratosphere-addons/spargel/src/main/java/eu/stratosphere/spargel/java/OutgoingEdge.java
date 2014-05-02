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
package eu.stratosphere.spargel.java;

/**
 * <tt>Edge</tt> objects represent edges between vertices. Edges are defined by their source and target
 * vertex id. Edges may have an associated value (for example a weight or a distance), if the
 * graph algorithm was initialized with the
 * {@link VertexCentricIteration#withEdgesWithValue(eu.stratosphere.api.java.DataSet, VertexUpdateFunction, MessagingFunction)}
 * method.
 *
 * @param <VertexKey> The type of the vertex key.
 * @param <EdgeValue> The type of the value associated with the edge. For scenarios where the edges do not hold
 *                    value, this type may be arbitrary.
 */
public final class OutgoingEdge<VertexKey extends Comparable<VertexKey>, EdgeValue> implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private VertexKey target;
	
	private EdgeValue edgeValue;
	
	void set(VertexKey target, EdgeValue edgeValue) {
		this.target = target;
		this.edgeValue = edgeValue;
	}
	
	/**
	 * Gets the target vertex id.
	 * 
	 * @return The target vertex id.
	 */
	public VertexKey target() {
		return target;
	}
	
	/**
	 * Gets the value associated with the edge. The value may be null if the iteration was initialized with
	 * an edge data set without edge values.
	 * Typical examples of edge values are weights or distances of the path represented by the edge.
	 *  
	 * @return The value associated with the edge.
	 */
	public EdgeValue edgeValue() {
		return edgeValue;
	}
}
