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

package eu.stratosphere.nephele.jobmanager;

import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionFailureException;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.template.InputSplit;

//TODO: Make this pluggable

/**
 * @author fhueske
 *         Assigns InputSplits to ExecutionVertices using a Priority Queue.
 */
public class InputSplitAssigner {

	private static InputSplitAssigner instance = null;

	/**
	 * Element of PriorityQueue. Elements are ordered by their number of assigned InputSplits.
	 * 
	 * @author fhueske
	 */
	private static class QueueElem implements Comparable<QueueElem> {
		private long noAssignedSplits;

		private long noLocalSplits;

		private long totalLengthOfAssignedSplits;

		private final ExecutionVertex vertex;

		public QueueElem(ExecutionVertex vertex, InputSplit[] splits) {
			this.totalLengthOfAssignedSplits = 0;
			this.noAssignedSplits = 0;
			this.vertex = vertex;
			this.noLocalSplits = 0;
			for (int i = 0; i < splits.length; i++) {
				if (this.hostsSplit(splits[i].getHostNames())) {
					noLocalSplits++;
				}
			}
		}

		public ExecutionVertex getVertex() {
			return this.vertex;
		}

		public void assignInputSplit(InputSplit inputSplit) {
			this.vertex.getEnvironment().addInputSplit(inputSplit);
			this.noAssignedSplits++;
			this.totalLengthOfAssignedSplits += inputSplit.getLength();
		}

		public long getNoAssignedSplits() {
			return this.noAssignedSplits;
		}

		public long getTotalLengthOfAssignedSplits() {
			return this.totalLengthOfAssignedSplits;
		}

		public long getNoLocalSplits() {
			return this.noLocalSplits;
		}

		public boolean hostsSplit(String[] splitLocations) {

			String hostName = this.vertex.getAllocatedResource().getInstance().getInstanceConnectionInfo()
				.getHostName();

			for (int i = 0; i < splitLocations.length; i++) {
				if (hostName.toLowerCase().equals(splitLocations[i].toLowerCase())) {
					return true;
				}
			}
			return false;
		}

		@Override
		public int compareTo(QueueElem arg0) {
			if (this.totalLengthOfAssignedSplits < arg0.getTotalLengthOfAssignedSplits()) {
				return -1;
			} else if (this.totalLengthOfAssignedSplits > arg0.getTotalLengthOfAssignedSplits()) {
				return 1;
			} else {
				if (this.noLocalSplits < arg0.getNoLocalSplits()) {
					return -1;
				} else if (this.noLocalSplits > arg0.getNoLocalSplits()) {
					return 1;
				} else {
					return 0;
				}
			}
		}

	}

	private PriorityQueue<QueueElem> vertexPrioQueue = new PriorityQueue<QueueElem>();

	private final Log LOG = LogFactory.getLog(InputSplitAssigner.class);

	/**
	 * Assigns input splits to all tasks of an ExecutionVertex
	 * 
	 * @param vertex
	 *        ExecutionVertex for which InputSplits will be assigned
	 * @return <code>false</code> if the instance assignment could not be done because at least one vertex has not yet
	 *         been assigned to a real instance, <code>true/code> otherwise
	 * @throws ExecutionFailureException
	 */
	public static boolean assignInputSplits(ExecutionVertex vertex) throws ExecutionFailureException {

		if (!vertex.isInputVertex()) {
			throw new ExecutionFailureException("Trying to assign splits to a NOT-InputSplit");
		}

		if (instance == null) {
			instance = new InputSplitAssigner();
		}

		return instance.assignInputSplits(vertex.getGroupVertex());
	}

	/*
	 * END modifications FH
	 */

	/**
	 * Initializes the PriorityQueue of ExecutionVertices of a ExecutionGroupVertex
	 * 
	 * @param graph
	 *        ExecutionGraph of the GroupVertex
	 * @param groupVertex
	 *        ExecutionGroupVertex who's ExecutionVertices are put into the PriorityQueue
	 * @throws ExecutionFailureException
	 */
	private void initializePriorityQueue(ExecutionGroupVertex groupVertex, InputSplit[] inputSplits)
			throws ExecutionFailureException {
		// for each ExecutionVertex
		for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); i++) {
			ExecutionVertex ev = groupVertex.getGroupMember(i);
			// Check if vertex was assigned to an instance
			if (ev.getAllocatedResource() == null) {
				throw new ExecutionFailureException("No instance assigned to vertex " + ev.getName());
			}
			// put vertex in priority queue
			this.vertexPrioQueue.add(new QueueElem(ev, inputSplits));
		}
	}

	/**
	 * Verifies that at least one InputSplit was assigned to each vertex
	 * 
	 * @throws ExecutionFailureException
	 */
	private void verifyAssignments() throws ExecutionFailureException {

		// for each ExecutionVertex
		while (this.vertexPrioQueue.size() > 0) {

			final QueueElem topElem = this.vertexPrioQueue.poll();
			// check if vertex has an input split assigned
			if (topElem.getNoAssignedSplits() == 0) {
				/*
				 * throw new ExecutionFailureException("Execution vertex "
				 * + topElem.getVertex().getName()
				 * + " on "
				 * + topElem.getVertex().getAllocatedResource().getInstance().getInstanceConnectionInfo()
				 * .getHostName() + " (" + topElem.getVertex().getID() + ") has no input split assigned");
				 */
				continue;
			}
			LOG.info(topElem.getNoAssignedSplits() + ": "
				+ topElem.getVertex().getAllocatedResource().getInstance().getInstanceConnectionInfo().getHostName());
//			for (InputSplit is : topElem.getVertex().getEnvironment().getInputSplits()) {
//				LOG.info("\t" + ((FileInputSplit) is).getPath());
//				for (String hn : is.getHostNames()) {
//					LOG.info("\t\t" + hn);
//				}
//			}

		}
	}

	/**
	 * Adds an InputSplit to the Vertex with least InputSplits assigned that can locally read the split.
	 * If the InputSplit cannot be read locally by any Vertex, it is assigned to the Vertex with least assigned
	 * InputSplits.
	 * 
	 * @param inputSplit
	 *        InputSplit that will be assigned
	 * @throws ExecutionFailureException
	 */
	private void addFileSplit(InputSplit inputSplit) throws ExecutionFailureException {

		// get locations of the split
		final String[] splitLocations = inputSplit.getHostNames();

		// check that the split has at least one location
		if (splitLocations.length == 0 && inputSplit.getLength() > 0) {
			throw new ExecutionFailureException("No known location for input splits " + inputSplit);
		}

		boolean added = false;
		// temp PriorityQueue
		final PriorityQueue<QueueElem> newVertexPrioQueue = new PriorityQueue<QueueElem>();

		// for each Vertex in the PrioQueue
		while (this.vertexPrioQueue.size() > 0) {
			// retrieve and remove top element of queue
			QueueElem topElem = this.vertexPrioQueue.poll();
			// check if split can be locally read by Vertex
			if (topElem.hostsSplit(splitLocations)) {
				// assign split to vertex
				topElem.assignInputSplit(inputSplit);
				added = true;
			}
			// add top element to temp PriorityQueue
			newVertexPrioQueue.add(topElem);
			// stop if split was assigned
			if (added) {
				break;
			}
		}
		// if split was assigned
		if (added) {
			// add all remaining vertices to temp PriorityQueue
			newVertexPrioQueue.addAll(this.vertexPrioQueue);
		} else {
			// priority queue was fully read but split was not assigned
			// -> split cannot be locally read by any vertex
			// assign split to the top element of the queue (vertex with least assigned splits)
			final QueueElem topElem = newVertexPrioQueue.poll();
			topElem.assignInputSplit(inputSplit);
			newVertexPrioQueue.add(topElem);
		}
		// switch queues
		this.vertexPrioQueue = newVertexPrioQueue;
	}

	/**
	 * Assign InputSplits for a ExecutionGroupVertex
	 * 
	 * @param graph
	 *        ExecutionGraph the ExecutionGroupVertex belongs to
	 * @param groupVertex
	 *        ExecutionGroupVertex who's InputSplits will be assigned
	 * @return <code>false</code> if the instance assignment could not be done because at least one vertex has not yet
	 *         been assigned to a real instance, <code>true/code> otherwise
	 * @throws ExecutionFailureException
	 */
	private boolean assignInputSplits(ExecutionGroupVertex groupVertex) throws ExecutionFailureException {

		for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); i++) {

			final AllocatedResource ar = groupVertex.getGroupMember(i).getAllocatedResource();
			if (ar.getInstance() instanceof DummyInstance) {
				return false;
			}
		}

		// get all InputSplits
		final InputSplit[] inputSplits = groupVertex.getInputSplits();
		// check that there are InputSplits
		if (inputSplits == null) {
			throw new ExecutionFailureException("Group vertex" + groupVertex.getName()
				+ " has no input splits assigned");
		}
		LOG.info("Number of input splits: " + inputSplits.length + " for " + groupVertex.getName());

		// initialize PriorityQueue for ExecutionGroupVertex
		initializePriorityQueue(groupVertex, inputSplits);

		// for each InputSplit
		for (int i = 0; i < inputSplits.length; i++) {
			// assign input split
			addFileSplit(inputSplits[i]);
		}

		// Check that every vertex has at least one split assigned
		verifyAssignments();

		// TODO: Repair assignments

		// empty prio queue for next assignment
		vertexPrioQueue.clear();

		return true;
	}

}
