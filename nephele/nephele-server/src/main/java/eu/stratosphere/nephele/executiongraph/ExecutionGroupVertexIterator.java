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

package eu.stratosphere.nephele.executiongraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * This class provides an implementation of the {@link Iterator} interface which allows to
 * traverse an execution graph and visit every reachable group vertex exactly once. The order
 * in which the group vertices are visited corresponds to the order of their discovery in a depth first
 * search.
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public class ExecutionGroupVertexIterator implements Iterator<ExecutionGroupVertex> {

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
	private final List<ExecutionGroupVertex> entryVertices = new ArrayList<ExecutionGroupVertex>();

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
	private final Set<ExecutionGroupVertex> alreadyVisited = new HashSet<ExecutionGroupVertex>();

	/**
	 * Auxiliary class which stores which vertices have already been visited.
	 * 
	 * @author warneke
	 */
	private static class TraversalEntry {

		/**
		 * The group vertex this entry is created for.
		 */
		private final ExecutionGroupVertex groupVertex;

		/**
		 * The current outgoing link of the group vertex.
		 */
		private int currentLink = 0;

		/**
		 * Constructs a new traversal entry.
		 * 
		 * @param groupVertex
		 *        the group vertex this traversal entry belongs to
		 * @param currentLink
		 *        the link index to use to visit the next group vertex
		 */
		public TraversalEntry(ExecutionGroupVertex groupVertex, int currentLink) {
			this.groupVertex = groupVertex;
			this.currentLink = currentLink;
		}

		/**
		 * Returns the group vertex this traversal entry belongs to.
		 * 
		 * @return the group vertex this traversal entry belongs to
		 */
		public ExecutionGroupVertex getGroupVertex() {
			return this.groupVertex;
		}

		/**
		 * Returns the link index to use to visit the next group vertex.
		 * 
		 * @return the link index to use to visit the next group vertex
		 */
		public int getCurrentLink() {
			return this.currentLink;
		}

		/**
		 * Increases the link index by one.
		 */
		public void increaseCurrentLink() {
			this.currentLink++;
		}
	}

	/**
	 * Creates a new execution group vertex iterator.
	 * 
	 * @param executionGraph
	 *        the execution graph that should be traversed
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to reverse it in
	 *        reverse order
	 * @param stage
	 *        the number of the stage whose vertices should be traversed or -1 if all stages should be included in the
	 *        traversal
	 */
	public ExecutionGroupVertexIterator(ExecutionGraph executionGraph, boolean forward, int stage) {

		this.forward = forward;
		this.stage = stage;

		// Collect start vertices
		if (stage < 0) {

			for (int i = 0; i < executionGraph.getNumberOfStages(); i++) {
				collectStartVertices(executionGraph.getStage(i));
			}
		} else {
			if (stage < executionGraph.getNumberOfStages()) {
				collectStartVertices(executionGraph.getStage(stage));
			}
		}

		if (this.entryVertices.size() > 0) {
			final TraversalEntry te = new TraversalEntry(this.entryVertices.get(0), 0);
			this.traversalStack.push(te);
			this.alreadyVisited.add(te.getGroupVertex());
		}
	}

	/**
	 * Collects all input group vertices (i.e. vertices with no incoming link or incoming links from other stages) in
	 * the given stage and adds them to an internal list.
	 * 
	 * @param stage
	 *        the number of the stage whose input vertices should be collected
	 */
	private void collectStartVertices(ExecutionStage stage) {

		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {

			final ExecutionGroupVertex groupVertex = stage.getStageMember(i);

			if (forward) {
				if ((groupVertex.getNumberOfBackwardLinks() == 0)
					|| ((this.stage >= 0) && allConnectionsFromOtherStage(groupVertex, true))) {
					this.entryVertices.add(groupVertex);
				}
			} else {
				if ((groupVertex.getNumberOfForwardLinks() == 0)
					|| ((this.stage >= 0) && allConnectionsFromOtherStage(groupVertex, false))) {
					this.entryVertices.add(groupVertex);
				}
			}
		}
	}

	/**
	 * Checks if for the given group vertex all incoming (if forward is <code>true</code>) or outgoing links (if forward
	 * is <code>false</code>) come from
	 * other stages than the one the given vertex is in.
	 * 
	 * @param groupVertex
	 *        the group vertex to check for
	 * @param forward
	 *        <code>true</code> if incoming links should be considered, <code>false</code> for outgoing links
	 * @return <code>true</code> if all incoming or outgoing links (depends on the forward switch) come from other
	 *         stages, <code>false</code> otherwise
	 */
	private boolean allConnectionsFromOtherStage(ExecutionGroupVertex groupVertex, boolean forward) {

		if (forward) {
			for (int i = 0; i < groupVertex.getNumberOfBackwardLinks(); i++) {
				if (this.stage == groupVertex.getBackwardEdge(i).getSourceVertex().getStageNumber()) {
					return false;
				}
			}
		} else {
			for (int i = 0; i < groupVertex.getNumberOfForwardLinks(); i++) {
				if (this.stage == groupVertex.getForwardEdge(i).getTargetVertex().getStageNumber()) {
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

			++this.numVisitedEntryVertices;
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
	public ExecutionGroupVertex next() {

		if (this.traversalStack.isEmpty()) {

			final TraversalEntry newentry = new TraversalEntry(this.entryVertices.get(this.numVisitedEntryVertices), 0);
			this.traversalStack.push(newentry);
			this.alreadyVisited.add(newentry.getGroupVertex());
		}

		final ExecutionGroupVertex returnVertex = this.traversalStack.peek().getGroupVertex();

		// Propose vertex to be visited next
		do {

			final TraversalEntry te = this.traversalStack.peek();

			// Check if we can traverse deeper into the graph
			final ExecutionGroupVertex candidateVertex = getCandidateVertex(te, forward);
			if (candidateVertex == null) {
				// Pop it from the stack
				traversalStack.pop();
			} else {
				// Create new entry and put it on the stack
				final TraversalEntry newte = new TraversalEntry(candidateVertex, 0);
				this.traversalStack.push(newte);
				this.alreadyVisited.add(newte.getGroupVertex());
				break;
			}

		} while (!this.traversalStack.isEmpty());

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
	private ExecutionGroupVertex getCandidateVertex(TraversalEntry te, boolean forward) {

		while (true) {

			if (forward) {
				// No more outgoing links to consider
				if (te.getCurrentLink() >= te.getGroupVertex().getNumberOfForwardLinks()) {
					break;
				}
			} else {
				// No more outgoing links to consider
				if (te.getCurrentLink() >= te.getGroupVertex().getNumberOfBackwardLinks()) {
					break;
				}
			}

			ExecutionGroupVertex tmp = null;
			if (forward) {
				tmp = te.getGroupVertex().getForwardEdge(te.getCurrentLink()).getTargetVertex();
			} else {
				tmp = te.getGroupVertex().getBackwardEdge(te.getCurrentLink()).getSourceVertex();
			}

			// Increase the current link index by one
			te.increaseCurrentLink();

			// If stage >= 0, tmp must be in the same stage as te.getGroupVertex()
			if (this.stage >= 0) {
				if (tmp.getStageNumber() != this.stage) {
					continue;
				}
			}

			if (!this.alreadyVisited.contains(tmp)) {
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
