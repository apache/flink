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

package eu.stratosphere.pact.example.graph.generator;

import java.util.HashSet;

/**
 * Node in the graph.
 */
public class TriangNode {

	/** Edges of this node. */
	private HashSet<TriangNode> edges;

	/** Identifier of this node. */
	private String name;

	/** Reference to the graph to hold control the edge count. */
	private TriangGraph graph;

	/**
	 * Ctr.
	 * 
	 * @param name
	 *        name of the node
	 * @param graph
	 *        graph this node belongs to
	 */
	public TriangNode(String name, TriangGraph graph) {

		this.name = name;
		this.graph = graph;
		this.edges = new HashSet<TriangNode>();

	}

	/**
	 * Returns the name of the node.
	 * 
	 * @return name of node
	 */
	public String getName() {

		return this.name;

	}

	/**
	 * Adds an edge to both this and the given node.
	 * 
	 * @param b
	 *        other end point of the edge
	 */
	public void addEdge(TriangNode b) {

		if (b == this) {

			System.err.println("Tried to add an edge from the node to itself.");

		} else if (b == null) {

			System.err.println("Adding edge to a null node.");

		} else {

			this.edges.add(b);
			b.edges.add(this);
			this.graph.edgesCount++;

		}

	}

	/**
	 * Returns all edges of this node.
	 * 
	 * @return edges of node
	 */
	public HashSet<TriangNode> getEdges() {

		return this.edges;

	}

	/**
	 * Checks whether the node has an edge to the given node.
	 * 
	 * @param b
	 *        node to which a connection shall be checked
	 * @return true if an edge exists, false otherwise
	 */
	public boolean hasEdgeTo(TriangNode b) {

		return this.edges.contains(b);

	}

	/**
	 * Returns the string representation of an edge.
	 * 
	 * @param start
	 *        first end point of the edge
	 * @param end
	 *        second end point of the edge
	 * @return string representation of the edge
	 */
	public static String getEdgeString(TriangNode start, TriangNode end) {

		return TriangNode.getEdgeString(start.getName(), end.getName());

	}

	/**
	 * Returns the string representation of an edge.
	 * 
	 * @param start
	 *        name of the first end point of the edge
	 * @param end
	 *        name of the second end point of the edge
	 * @return string representation of the edge
	 */
	public static String getEdgeString(String start, String end) {

		if (start.compareTo(end) < 0)
			return start + "|" + end + "|";
		else
			return end + "|" + start + "|";

	}

}