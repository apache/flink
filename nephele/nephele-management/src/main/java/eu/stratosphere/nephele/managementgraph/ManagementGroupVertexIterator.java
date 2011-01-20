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

package eu.stratosphere.nephele.managementgraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * This class provides an implementation of the {@link Iterator} interface which allows to
 * traverse a management graph and visit every reachable group vertex exactly once. The order
 * in which the group vertices are visited corresponds to the order of their discovery in a depth first
 * search.
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGroupVertexIterator implements Iterator<ManagementGroupVertex> {

	/**
	 * Stores whether the group graph is traversed starting from the input or the output vertices.
	 */
	private final boolean forward;

	/**
	 * The stage to traverse, -1 to traverse all stages of the graph.
	 */
	private final int stage;

	/**
	 * List of entry vertices for the traversal (either input or output vertices).
	 */
	private final List<ManagementGroupVertex> entryVertices = new ArrayList<ManagementGroupVertex>();

	/**
	 * Number of already visited entry vertices.
	 */
	private int numVisitedEntryVertices = 0;

	/**
	 * Stack used for the traversal.
	 */
	private final Stack<TraversalEntry> traversalStack = new Stack<TraversalEntry>();

	/**
	 * Set storing the vertices already visited during traversal.
	 */
	private final Set<ManagementGroupVertex> alreadyVisited = new HashSet<ManagementGroupVertex>();

	/**
	 * Auxiliary class which stores which vertices have already been visited.
	 * 
	 * @author warneke
	 */
	private static class TraversalEntry {

		/**
		 * The group vertex this entry is created for.
		 */
		private final ManagementGroupVertex groupVertex;

		/**
		 * The current outgoing edge of the group vertex.
		 */
		private int currentEdge = 0;

		/**
		 * Constructs a new traversal entry.
		 * 
		 * @param groupVertex
		 *        the group vertex this traversal entry belongs to
		 * @param currentEdge
		 *        the edge index to use to visit the next group vertex
		 */
		public TraversalEntry(final ManagementGroupVertex groupVertex, final int currentEdge) {
			this.groupVertex = groupVertex;
			this.currentEdge = currentEdge;
		}

		/**
		 * Returns the group vertex this traversal entry belongs to.
		 * 
		 * @return the group vertex this traversal entry belongs to
		 */
		public ManagementGroupVertex getGroupVertex() {
			return this.groupVertex;
		}

		/**
		 * Returns the edge index to use to visit the next group vertex.
		 * 
		 * @return the edge index to use to visit the next group vertex
		 */
		public int getCurrentEdge() {
			return this.currentEdge;
		}

		/**
		 * Increases the edge index by one.
		 */
		public void increaseCurrentEdge() {
			this.currentEdge++;
		}
	}

	/**
	 * Creates a new management group vertex iterator.
	 * 
	 * @param managementGraph
	 *        the management graph that should be traversed
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to reverse it in
	 *        reverse order
	 * @param stage
	 *        the number of the stage whose vertices should be traversed or -1 if all stages should be included in the
	 *        traversal
	 */
	public ManagementGroupVertexIterator(final ManagementGraph managementGraph, final boolean forward, final int stage) {

		this.forward = forward;
		this.stage = stage;

		// Collect start vertices
		if (stage < 0) {

			for (int i = 0; i < managementGraph.getNumberOfStages(); i++) {
				collectStartVertices(managementGraph.getStage(i));
			}
		} else {
			if (stage < managementGraph.getNumberOfStages()) {
				collectStartVertices(managementGraph.getStage(stage));
			}
		}

		if (this.entryVertices.size() > 0) {
			final TraversalEntry te = new TraversalEntry(this.entryVertices.get(0), 0);
			traversalStack.push(te);
		}
	}

	/**
	 * Collects all input group vertices (i.e. vertices with no incoming edge or incoming edges from other stages) in
	 * the given stage and adds them to an internal list.
	 * 
	 * @param stage
	 *        the number of the stage whose input vertices should be collected
	 */
	private void collectStartVertices(final ManagementStage stage) {

		for (int i = 0; i < stage.getNumberOfGroupVertices(); i++) {

			final ManagementGroupVertex groupVertex = stage.getGroupVertex(i);

			if (forward) {
				if ((groupVertex.getNumberOfBackwardEdges() == 0)
					|| ((this.stage >= 0) && allConnectionsFromOtherStage(groupVertex, true))) {
					this.entryVertices.add(groupVertex);
				}
			} else {
				if ((groupVertex.getNumberOfForwardEdges() == 0)
					|| ((this.stage >= 0) && allConnectionsFromOtherStage(groupVertex, false))) {
					this.entryVertices.add(groupVertex);
				}
			}
		}
	}

	/**
	 * Checks if for the given group vertex all incoming (if forward is <code>true</code>) or outgoing edges (if forward
	 * is <code>false</code>) come from
	 * other stages than the one the given vertex is in.
	 * 
	 * @param groupVertex
	 *        the group vertex to check for
	 * @param forward
	 *        <code>true</code> if incoming edges should be considered, <code>false</code> for outgoing edges
	 * @return <code>true</code> if all incoming or outgoing edges (depends on the forward switch) come from other
	 *         stages, <code>false</code> otherwise
	 */
	private boolean allConnectionsFromOtherStage(final ManagementGroupVertex groupVertex, final boolean forward) {

		if (forward) {
			for (int i = 0; i < groupVertex.getNumberOfBackwardEdges(); i++) {
				if (this.stage == groupVertex.getBackwardEdge(i).getSource().getStageNumber()) {
					return false;
				}
			}
		} else {
			for (int i = 0; i < groupVertex.getNumberOfForwardEdges(); i++) {
				if (this.stage == groupVertex.getForwardEdge(i).getTarget().getStageNumber()) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {

		if (this.traversalStack.isEmpty()) {

			this.numVisitedEntryVertices++;
			if (this.entryVertices.size() <= this.numVisitedEntryVertices) {
				return false;
			}
		}

		return true;

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ManagementGroupVertex next() {

		if (this.traversalStack.isEmpty()) {

			final TraversalEntry newentry = new TraversalEntry(this.entryVertices.get(this.numVisitedEntryVertices), 0);
			this.traversalStack.push(newentry);
		}

		final ManagementGroupVertex returnVertex = this.traversalStack.peek().getGroupVertex();

		// Propose vertex to be visited next
		do {

			final TraversalEntry te = this.traversalStack.peek();

			// Check if we can traverse deeper into the graph
			final ManagementGroupVertex candidateVertex = getCandidateVertex(te, forward);
			if (candidateVertex == null) {
				// Pop it from the stack
				this.traversalStack.pop();
			} else {
				// Create new entry and put it on the stack
				final TraversalEntry newte = new TraversalEntry(candidateVertex, 0);
				this.traversalStack.add(newte);
				break;
			}

		} while (!this.traversalStack.isEmpty());

		// Mark vertex as already visited
		this.alreadyVisited.add(returnVertex);

		return returnVertex;

	}

	/**
	 * Returns a candidate group vertex which could potentially be visited next because it is reachable from the
	 * currently considered group vertex.
	 * 
	 * @param te
	 *        the traversal entry for the current source group vertex
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 * @return a candidate group vertex which could potentially be visited next
	 */
	private ManagementGroupVertex getCandidateVertex(TraversalEntry te, boolean forward) {

		while (true) {

			if (forward) {
				// No more outgoing edges to consider
				if (te.getCurrentEdge() >= te.getGroupVertex().getNumberOfForwardEdges()) {
					break;
				}
			} else {
				// No more outgoing edges to consider
				if (te.getCurrentEdge() >= te.getGroupVertex().getNumberOfBackwardEdges()) {
					break;
				}
			}

			ManagementGroupVertex tmp = null;
			if (forward) {
				tmp = te.getGroupVertex().getForwardEdge(te.getCurrentEdge()).getTarget();
			} else {
				tmp = te.getGroupVertex().getBackwardEdge(te.getCurrentEdge()).getSource();
			}

			// Increase the current edge index by one
			te.increaseCurrentEdge();

			// If stage >= 0, tmp must be in the same stage as te.getGroupVertex()
			if (stage >= 0) {
				if (tmp.getStageNumber() != stage) {
					continue;
				}
			}

			if (!alreadyVisited.contains(tmp)) {
				return tmp;
			}
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() {
		// According to the documentation this method is optional and does not need to be implemented

	}

}
