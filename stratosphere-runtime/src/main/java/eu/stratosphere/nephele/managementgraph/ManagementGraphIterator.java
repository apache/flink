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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class provides an implementation of the {@link java.util.Iterator} interface which allows to
 * traverse a management graph and visit every reachable vertex exactly once. The order
 * in which the vertices are visited corresponds to the order of their discovery in a depth first
 * search.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class ManagementGraphIterator implements Iterator<ManagementVertex> {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ManagementGraphIterator.class);

	/**
	 * The management graph this iterator traverses.
	 */
	private final ManagementGraph managementGraph;

	/**
	 * Stores whether the graph is traversed starting from the input or the output vertices.
	 */
	private final boolean forward;

	/**
	 * The stage that should be traversed by this iterator.
	 */
	private final int startStage;

	/**
	 * Stores whether the iterator is confined to the start stage or not.
	 */
	private final boolean confinedToStage;

	/**
	 * The number of visited vertices from the entry set (either input or output vertices).
	 */
	private int numVisitedEntryVertices = 0;

	/**
	 * Stack used for the depth first search.
	 */
	private final Stack<TraversalEntry> traversalStack = new Stack<TraversalEntry>();

	/**
	 * Set of already visited vertices during traversal.
	 */
	private final Set<ManagementVertex> alreadyVisited = new HashSet<ManagementVertex>();

	/**
	 * Auxiliary class which stores which vertices have already been visited.
	 * 
	 * @author warneke
	 */
	private static class TraversalEntry {

		/**
		 * Management vertex this entry has been created for.
		 */
		private final ManagementVertex managementVertex;

		/**
		 * Next gate to traverse.
		 */
		private int currentGate;

		/**
		 * Next channel to traverse.
		 */
		private int currentChannel;

		/**
		 * Constructs a new traversal entry.
		 * 
		 * @param managementVertex
		 *        the management vertex this entry belongs to
		 * @param currentGate
		 *        the gate index to use to visit the next vertex
		 * @param currentChannel
		 *        the channel index to use to visit the next vertex
		 */
		public TraversalEntry(final ManagementVertex managementVertex, final int currentGate, final int currentChannel) {
			this.managementVertex = managementVertex;
			this.currentGate = currentGate;
			this.currentChannel = currentChannel;
		}

		/**
		 * Returns the management vertex this entry belongs to.
		 * 
		 * @return the managenemt vertex this entry belongs to
		 */
		public ManagementVertex getManagementVertex() {
			return this.managementVertex;
		}

		/**
		 * Returns the gate index to use to visit the next vertex.
		 * 
		 * @return the gate index to use to visit the next vertex
		 */
		public int getCurrentGate() {
			return this.currentGate;
		}

		/**
		 * Returns the channel index to use to visit the next vertex.
		 * 
		 * @return the channel index to use to visit the next vertex
		 */
		public int getCurrentChannel() {
			return this.currentChannel;
		}

		/**
		 * Increases the channel index by one.
		 */
		public void increaseCurrentChannel() {
			++this.currentChannel;
		}

		/**
		 * Increases the gate index by one.
		 */
		public void increaseCurrentGate() {
			++this.currentGate;
		}

		/**
		 * Resets the channel index.
		 */
		public void resetCurrentChannel() {
			this.currentChannel = 0;
		}

	}

	/**
	 * Creates a new management graph iterator.
	 * 
	 * @param managementGraph
	 *        the management graph that should be traversed
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 */
	public ManagementGraphIterator(final ManagementGraph managementGraph, final boolean forward) {
		this(managementGraph, forward ? 0 : (managementGraph.getNumberOfStages() - 1), false, forward);
	}

	/**
	 * Creates a new management graph iterator.
	 * 
	 * @param managementGraph
	 *        the management graph that should be traversed
	 * @param startStage
	 *        the index of the stage of the graph where the traversal is supposed to begin
	 * @param confinedToStage
	 *        <code>false</code> if the graph iterator is allowed to traverse to upper (in case of reverse order
	 *        traversal lower) stages, <code>true</code> otherwise.
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 */
	public ManagementGraphIterator(final ManagementGraph managementGraph, final int startStage,
			final boolean confinedToStage, final boolean forward) {

		this.managementGraph = managementGraph;
		this.forward = forward;
		this.startStage = startStage;
		this.confinedToStage = confinedToStage;

		if (startStage >= this.managementGraph.getNumberOfStages()) {
			return;
		}

		if (forward) {
			if (managementGraph.getNumberOfInputVertices(startStage) > 0) {

				final TraversalEntry te = new TraversalEntry(managementGraph.getInputVertex(startStage, 0), 0, 0);
				this.traversalStack.push(te);
				this.alreadyVisited.add(te.getManagementVertex());

			}
		} else {
			if (managementGraph.getNumberOfOutputVertices(startStage) > 0) {

				final TraversalEntry te = new TraversalEntry(managementGraph.getOutputVertex(startStage, 0), 0, 0);
				this.traversalStack.push(te);
				this.alreadyVisited.add(te.getManagementVertex());
			}

		}
	}

	/**
	 * Creates a new management graph iterator. This constructor can be used to
	 * traverse only specific parts of the graph starting at <code>startVertex</code>.
	 * The iterator will not switch to the next input/output vertex of an output/input vertex
	 * has been reached.
	 * 
	 * @param managementGraph
	 *        the management graph that should be traversed
	 * @param startVertex
	 *        the vertex to start the traversal from
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to reverse it in
	 *        reverse order
	 */
	public ManagementGraphIterator(final ManagementGraph managementGraph, final ManagementVertex startVertex,
			final boolean forward) {

		this.managementGraph = managementGraph;
		this.forward = forward;
		this.numVisitedEntryVertices = -1;
		this.startStage = 0;
		this.confinedToStage = false;

		final TraversalEntry te = new TraversalEntry(startVertex, 0, 0);
		this.traversalStack.push(te);
		this.alreadyVisited.add(te.getManagementVertex());
	}


	@Override
	public boolean hasNext() {

		if (this.traversalStack.isEmpty()) {

			if (this.numVisitedEntryVertices < 0) {
				// User chose a specific starting vertex
				return false;
			}

			++this.numVisitedEntryVertices;

			if (this.forward) {
				if (this.managementGraph.getNumberOfInputVertices(this.startStage) <= this.numVisitedEntryVertices) {
					return false;
				}
			} else {
				if (this.managementGraph.getNumberOfOutputVertices(this.startStage) <= this.numVisitedEntryVertices) {
					return false;
				}
			}
		}

		return true;
	}


	@Override
	public ManagementVertex next() {

		if (this.traversalStack.isEmpty()) {

			if (this.numVisitedEntryVertices < 0) {
				// User chose a specific entry vertex
				return null;
			}

			TraversalEntry newentry;

			if (this.forward) {
				newentry = new TraversalEntry(this.managementGraph.getInputVertex(this.startStage, this.numVisitedEntryVertices),
					0, 0);
			} else {
				newentry = new TraversalEntry(
					managementGraph.getOutputVertex(this.startStage, this.numVisitedEntryVertices), 0, 0);
			}

			this.traversalStack.push(newentry);
			this.alreadyVisited.add(newentry.getManagementVertex());
		}

		final ManagementVertex returnVertex = this.traversalStack.peek().getManagementVertex();

		// Propose vertex to be visited next
		do {

			final TraversalEntry te = this.traversalStack.peek();

			// Check if we can traverse deeper into the graph
			final ManagementVertex candidateVertex = getCandidateVertex(te, this.forward);
			if (candidateVertex == null) {
				// Pop it from the stack
				this.traversalStack.pop();
			} else {
				// Create new entry and put it on the stack
				final TraversalEntry newte = new TraversalEntry(candidateVertex, 0, 0);
				this.traversalStack.push(newte);
				this.alreadyVisited.add(candidateVertex);
				break;
			}

		} while (!this.traversalStack.isEmpty());

		return returnVertex;
	}

	/**
	 * Returns a candidate vertex which could potentially be visited next because it is reachable from the
	 * currently considered vertex.
	 * 
	 * @param te
	 *        the traversal entry for the current source vertex
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 * @return a candidate vertex which could potentially be visited next
	 */
	private ManagementVertex getCandidateVertex(final TraversalEntry te, final boolean forward) {

		if (forward) {

			while (true) {

				if (this.confinedToStage && te.getCurrentChannel() == 0) {
					while (currentGateLeadsToOtherStage(te, this.forward)) {
						te.increaseCurrentGate();
					}
				}

				// No more outgoing edges to consider
				if (te.getCurrentGate() >= te.getManagementVertex().getNumberOfOutputGates()) {
					break;
				}

				if (te.getCurrentChannel() >= te.getManagementVertex().getOutputGate(te.getCurrentGate())
					.getNumberOfForwardEdges()) {
					te.increaseCurrentGate();
					te.resetCurrentChannel();
				} else {
					final ManagementEdge forwardEdge = te.getManagementVertex().getOutputGate(te.getCurrentGate())
						.getForwardEdge(te.getCurrentChannel());
					final ManagementVertex target = forwardEdge.getTarget().getVertex();
					te.increaseCurrentChannel();
					if (!alreadyVisited.contains(target)) {
						return target;
					}
				}
			}
		} else {

			while (true) {

				if (this.confinedToStage && te.getCurrentChannel() == 0) {
					while (currentGateLeadsToOtherStage(te, this.forward)) {
						te.increaseCurrentGate();
					}
				}

				// No more incoming edges to consider
				if (te.getCurrentGate() >= te.getManagementVertex().getNumberOfInputGates()) {
					break;
				}

				if (te.getCurrentChannel() >= te.getManagementVertex().getInputGate(te.getCurrentGate())
					.getNumberOfBackwardEdges()) {
					te.increaseCurrentGate();
					te.resetCurrentChannel();
				} else {
					final ManagementEdge backwardEdge = te.getManagementVertex().getInputGate(te.getCurrentGate())
						.getBackwardEdge(te.getCurrentChannel());
					final ManagementVertex source = backwardEdge.getSource().getVertex();
					if (source == null) {
						LOG.error("Inconsistency in vertex map found (backward)!");
					}
					te.increaseCurrentChannel();
					if (!this.alreadyVisited.contains(source)) {
						return source;
					}
				}
			}
		}

		return null;
	}

	/**
	 * Checks if the current gate leads to another stage or not.
	 * 
	 * @param te
	 *        the current traversal entry
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 * @return <code>true</code> if current gate leads to another stage, otherwise <code>false</code>
	 */
	private boolean currentGateLeadsToOtherStage(final TraversalEntry te, final boolean forward) {

		final ManagementGroupVertex groupVertex = te.getManagementVertex().getGroupVertex();

		if (forward) {

			if (te.getCurrentGate() >= groupVertex.getNumberOfForwardEdges()) {
				return false;
			}

			final ManagementGroupEdge edge = groupVertex.getForwardEdge(te.getCurrentGate());
			if (edge.getTarget().getStageNumber() == groupVertex.getStageNumber()) {
				return false;
			}

		} else {

			if (te.getCurrentGate() >= groupVertex.getNumberOfBackwardEdges()) {
				return false;
			}

			final ManagementGroupEdge edge = groupVertex.getBackwardEdge(te.getCurrentGate());
			if (edge.getSource().getStageNumber() == groupVertex.getStageNumber()) {
				return false;
			}
		}

		return true;
	}


	@Override
	public void remove() {
		// According to the Iterator documentation this method is optional.
	}

}
