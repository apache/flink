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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Class grouping nodes together to a graph.
 */
public class TriangGraph {

	/** Nodes of the graph. */
	protected ArrayList<TriangNode> nodes = new ArrayList<TriangNode>();

	/** Number of triangles in the graph. */
	protected int triangleCount = 0;

	/** Number of edges in the graph modified by each TriangNode adding an edge. */
	protected int edgesCount = 0;

	/**
	 * Add a node to the graph.
	 * 
	 * @param a
	 *        node to add
	 */
	public void addNode(TriangNode a) {

		this.nodes.add(a);

	}

	/**
	 * Returns the number of nodes of the graph.
	 * 
	 * @return number of nodes
	 */
	public int getNodeCount() {

		return this.nodes.size();

	}

	/**
	 * Returns the node of the graph at the specified index.
	 * 
	 * @param index
	 *        index of desired node
	 * @return node or null if index > size()
	 */
	public TriangNode getNode(int index) {

		if (index < this.nodes.size())
			return this.nodes.get(index);
		else
			return null;

	}

	/**
	 * Calculates the triangles that form by adding an edge.
	 * 
	 * @param start
	 *        first end of edge
	 * @param end
	 *        second end of edge
	 */
	public Collection<String> calculateTriangles(TriangNode start, TriangNode end) {

		if (start == end) {
			System.err.println("calculate Triangles called with same node.");
			return new HashSet<String>();
		}

		HashSet<String> triangStrings = new HashSet<String>();
		HashSet<TriangNode> startEdges = start.getEdges();
		Iterator<TriangNode> it = startEdges.iterator();
		while (it.hasNext()) {

			TriangNode node = it.next();
			if (end.hasEdgeTo(node)) {

				// both nodes (now connected by an edge) have an edge to the
				// same third node => triangle
				this.triangleCount++;
				triangStrings.add(TriangGraph.getTriangleString(start, end, node));

			}

		}

		return triangStrings;
	}

	/**
	 * Returns the String representation of a triangle.
	 * 
	 * @param node1
	 *        first node of triangle
	 * @param node2
	 *        second node of triangle
	 * @param node3
	 *        third node of triangle
	 * @return string representation of a triangle
	 */
	public static String getTriangleString(TriangNode node1, TriangNode node2, TriangNode node3) {

		// get names of nodes of triangle
		String name1 = node1.getName();
		String name2 = node2.getName();
		String name3 = node3.getName();

		// use linked list to sort the names in ascending order
		LinkedList<String> list = new LinkedList<String>();
		if (name1.compareTo(name2) < 0) {

			list.add(name1);
			list.addLast(name2);

		} else {

			list.add(name2);
			list.addLast(name1);

		}

		if (name3.compareTo(list.get(0)) < 0) {

			list.addFirst(name3);

		} else if (name3.compareTo(list.get(1)) < 0) {

			list.add(1, name3);

		} else {

			list.addLast(name3);

		}

		// format string for triad according to pacts program
		return TriangNode.getEdgeString(list.get(0), list.get(1)) + TriangNode.getEdgeString(list.get(0), list.get(2))
			+ TriangNode.getEdgeString(list.get(1), list.get(2));

	}

}
