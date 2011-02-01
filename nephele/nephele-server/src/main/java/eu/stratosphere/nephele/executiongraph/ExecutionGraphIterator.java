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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.types.Record;

/**
 * This class provides an implementation of the {@link Iterator} interface which allows to
 * traverse an execution graph and visit every reachable vertex exactly once. The order
 * in which the vertices are visited corresponds to the order of their discovery in a depth first
 * search.
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public class ExecutionGraphIterator implements Iterator<ExecutionVertex> {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(ExecutionGraphIterator.class);

	/**
	 * The execution this iterator traverses.
	 */
	private final ExecutionGraph executionGraph;

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
	private final Set<ExecutionVertex> alreadyVisited = new HashSet<ExecutionVertex>();

	/**
	 * Auxiliary class which stores which vertices have already been visited.
	 * 
	 * @author warneke
	 */
	private static class TraversalEntry {

		/**
		 * Execution vertex this entry has been created for.
		 */
		private final ExecutionVertex executionVertex;

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
		 * @param executionVertex
		 *        the execution vertex this entry belongs to
		 * @param currentGate
		 *        the gate index to use to visit the next vertex
		 * @param currentChannel
		 *        the channel index to use to visit the next vertex
		 */
		public TraversalEntry(ExecutionVertex executionVertex, int currentGate, int currentChannel) {
			this.executionVertex = executionVertex;
			this.currentGate = currentGate;
			this.currentChannel = currentChannel;
		}

		/**
		 * Returns the execution vertex this entry belongs to.
		 * 
		 * @return the execution vertex this entry belongs to
		 */
		public ExecutionVertex getExecutionVertex() {
			return this.executionVertex;
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
			this.currentChannel++;
		}

		/**
		 * Increases the gate index by one.
		 */
		public void increaseCurrentGate() {
			this.currentGate++;
		}

		/**
		 * Resets the channel index.
		 */
		public void resetCurrentChannel() {
			this.currentChannel = 0;
		}

	}

	/**
	 * Creates a new execution graph iterator.
	 * 
	 * @param executionGraph
	 *        the execution graph that should be traversed
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 */
	public ExecutionGraphIterator(ExecutionGraph executionGraph, boolean forward) {
		this(executionGraph, forward ? 0 : (executionGraph.getNumberOfStages() - 1), false, forward);
	}

	/**
	 * Creates a new execution graph iterator.
	 * 
	 * @param executionGraph
	 *        the execution graph that should be traversed
	 * @param startStage
	 *        the index of the stage of the graph where the traversal is supposed to begin
	 * @param confinedToStage
	 *        <code>false</code> if the graph iterator is allowed to traverse to upper (in case of reverse order
	 *        traversal lower) stages, <code>true</code> otherwise.
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to traverse it in
	 *        reverse order
	 */
	public ExecutionGraphIterator(ExecutionGraph executionGraph, int startStage, boolean confinedToStage,
			boolean forward) {

		this.executionGraph = executionGraph;
		this.forward = forward;
		this.startStage = startStage;
		this.confinedToStage = confinedToStage;

		if (startStage >= this.executionGraph.getNumberOfStages()) {
			return;
		}

		if (forward) {
			if (executionGraph.getNumberOfInputVertices(startStage) > 0) {

				final TraversalEntry te = new TraversalEntry(executionGraph.getInputVertex(startStage, 0), 0, 0);
				traversalStack.push(te);

			}
		} else {
			if (executionGraph.getNumberOfOutputVertices(startStage) > 0) {

				final TraversalEntry te = new TraversalEntry(executionGraph.getOutputVertex(startStage, 0), 0, 0);
				traversalStack.push(te);
			}

		}
	}

	/**
	 * Creates a new execution graph iterator. This constructor can be used to
	 * traverse only specific parts of the graph starting at <code>startVertex</code>.
	 * The iterator will not switch to the next input/output vertex of an output/input vertex
	 * has been reached.
	 * 
	 * @param executionGraph
	 *        the execution graph that should be traversed
	 * @param startVertex
	 *        the vertex to start the traversal from
	 * @param forward
	 *        <code>true</code> if the graph should be traversed in correct order, <code>false</code> to reverse it in
	 *        reverse order
	 */
	public ExecutionGraphIterator(ExecutionGraph executionGraph, ExecutionVertex startVertex, boolean forward) {

		this.executionGraph = executionGraph;
		this.forward = forward;
		this.numVisitedEntryVertices = -1;
		this.startStage = 0;
		this.confinedToStage = false;

		final TraversalEntry te = new TraversalEntry(startVertex, 0, 0);
		traversalStack.push(te);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {

		if (traversalStack.isEmpty()) {

			if (numVisitedEntryVertices < 0) {
				// User chose a specific starting vertex
				return false;
			}

			numVisitedEntryVertices++;

			if (forward) {
				if (executionGraph.getNumberOfInputVertices(this.startStage) <= numVisitedEntryVertices) {
					return false;
				}
			} else {
				if (executionGraph.getNumberOfOutputVertices(this.startStage) <= numVisitedEntryVertices) {
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
	public ExecutionVertex next() {

		if (traversalStack.isEmpty()) {

			if (numVisitedEntryVertices < 0) {
				// User chose a specific entry vertex
				return null;
			}

			TraversalEntry newentry;

			if (forward) {
				newentry = new TraversalEntry(executionGraph.getInputVertex(this.startStage, numVisitedEntryVertices),
					0, 0);
			} else {
				newentry = new TraversalEntry(executionGraph.getOutputVertex(this.startStage, numVisitedEntryVertices),
					0, 0);
			}

			traversalStack.push(newentry);
		}

		final ExecutionVertex returnVertex = traversalStack.peek().getExecutionVertex();

		// Propose vertex to be visited next
		do {

			final TraversalEntry te = traversalStack.peek();

			// Check if we can traverse deeper into the graph
			final ExecutionVertex candidateVertex = getCandidateVertex(te, forward);
			if (candidateVertex == null) {
				// Pop it from the stack
				traversalStack.pop();
			} else {
				// Create new entry and put it on the stack
				final TraversalEntry newte = new TraversalEntry(candidateVertex, 0, 0);
				traversalStack.add(newte);
				break;
			}

		} while (!traversalStack.isEmpty());

		// Mark vertex as already visited
		alreadyVisited.add(returnVertex);

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
	private ExecutionVertex getCandidateVertex(TraversalEntry te, boolean forward) {

		if (forward) {

			while (true) {

				if (this.confinedToStage && te.getCurrentChannel() == 0) {
					while (currentGateLeadsToOtherStage(te, this.forward)) {
						te.increaseCurrentGate();
					}
				}

				// No more outgoing edges to consider
				if (te.getCurrentGate() >= te.getExecutionVertex().getEnvironment().getNumberOfOutputGates()) {
					break;
				}

				if (te.getCurrentChannel() >= te.getExecutionVertex().getEnvironment().getOutputGate(
					te.getCurrentGate()).getNumberOfOutputChannels()) {
					te.increaseCurrentGate();
					te.resetCurrentChannel();
				} else {
					final AbstractOutputChannel<? extends Record> outputChannel = te.getExecutionVertex()
						.getEnvironment().getOutputGate(te.getCurrentGate()).getOutputChannel(te.getCurrentChannel());
					final ExecutionVertex tmp = executionGraph.getVertexByChannelID(outputChannel
						.getConnectedChannelID());
					if (tmp == null) {
						LOG.error("Inconsistency in vertex map found (forward)!");
					}
					te.increaseCurrentChannel();
					if (!alreadyVisited.contains(tmp)) {
						return tmp;
					}
				}
			}
		} else {

			while (true) {

				// No more outgoing edges to consider
				if (te.getCurrentGate() >= te.getExecutionVertex().getEnvironment().getNumberOfInputGates()) {
					break;
				}

				if (te.getCurrentChannel() >= te.getExecutionVertex().getEnvironment()
					.getInputGate(te.getCurrentGate()).getNumberOfInputChannels()) {
					te.increaseCurrentGate();
					te.resetCurrentChannel();
				} else {
					final AbstractInputChannel<? extends Record> inputChannel = te.getExecutionVertex()
						.getEnvironment().getInputGate(te.getCurrentGate()).getInputChannel(te.getCurrentChannel());
					final ExecutionVertex tmp = executionGraph.getVertexByChannelID(inputChannel
						.getConnectedChannelID());
					if (tmp == null) {
						LOG.error("Inconsistency in vertex map found (backward)!");
					}
					te.increaseCurrentChannel();
					if (!alreadyVisited.contains(tmp)) {
						return tmp;
					}
				}
			}
		}

		return null;
	}

	private boolean currentGateLeadsToOtherStage(TraversalEntry te, boolean forward) {

		final ExecutionGroupVertex groupVertex = te.getExecutionVertex().getGroupVertex();

		if (forward) {

			if (te.getCurrentGate() >= groupVertex.getNumberOfForwardLinks()) {
				return false;
			}

			final ExecutionGroupEdge edge = groupVertex.getForwardEdge(te.getCurrentGate());
			if (edge.getTargetVertex().getStageNumber() == groupVertex.getStageNumber()) {
				return false;
			}

		} else {
			if (te.getCurrentGate() >= groupVertex.getNumberOfBackwardLinks()) {
				return false;
			}

			final ExecutionGroupEdge edge = groupVertex.getBackwardEdge(te.getCurrentGate());
			if (edge.getSourceVertex().getStageNumber() == groupVertex.getStageNumber()) {
				return false;
			}
		}

		return true;
	}

	/*
	 * private void increaseCurrentGate(TraversalEntry te) {
	 * if(this.stage < 0) {
	 * te.increaseCurrentGate();
	 * return;
	 * }
	 * final ExecutionGroupVertex groupVertex = te.getExecutionVertex().getGroupVertex();
	 * while(true) {
	 * te.increaseCurrentGate();
	 * if(this.forward) {
	 * if(groupVertex.getNumberOfForwardLinks() >= te.getCurrentGate()) {
	 * break;
	 * }
	 * //Skip the gate if it would lead to another stage
	 * final ExecutionGroupEdge edge = groupVertex.getForwardEdge(te.getCurrentGate());
	 * if(edge.getTargetVertex().getStageNumber() == groupVertex.getStageNumber()) {
	 * break;
	 * }
	 * } else {
	 * if(groupVertex.getNumberOfBackwardLinks() >= te.getCurrentGate()) {
	 * break;
	 * }
	 * //Skip the gate if it would lead to another stage
	 * final ExecutionGroupEdge edge = groupVertex.getBackwardEdge(te.getCurrentGate());
	 * if(edge.getSourceVertex().getStageNumber() == groupVertex.getStageNumber()) {
	 * break;
	 * }
	 * }
	 * }
	 * }
	 */

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() {
		// According to the Iterator documentation this method is optional.
	}

}
