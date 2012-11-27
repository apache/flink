/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.jobmanager.splitassigner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertexIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.template.InputSplit;

/**
 * The input split tracker maintains a log of all the input splits that are handed out to the individual input vertices.
 * In case of an input vertex must be restarted the input split tracker makes sure that the vertex receives the same
 * sequence of input splits as in its original run up to the point that it crashed.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
final class InputSplitTracker {

	/**
	 * The logging object which is used to report information and errors.
	 */
	private static final Log LOG = LogFactory.getLog(InputSplitTracker.class);

	/**
	 * The central split map which stores the logs of the individual input vertices.
	 */
	private final ConcurrentMap<ExecutionVertexID, List<InputSplit>> splitMap = new ConcurrentHashMap<ExecutionVertexID, List<InputSplit>>();

	/**
	 * Constructor with package visibility only.
	 */
	InputSplitTracker() {
	}

	/**
	 * Registers a new job with the input split tracker.
	 * 
	 * @param eg
	 *        the execution graph of the job to be registered
	 */
	void registerJob(final ExecutionGraph eg) {

		final Iterator<ExecutionGroupVertex> it = new ExecutionGroupVertexIterator(eg, true, -1);
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			final InputSplit[] inputSplits = groupVertex.getInputSplits();

			if (inputSplits == null) {
				continue;
			}

			if (inputSplits.length == 0) {
				continue;
			}

			for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); ++i) {
				final ExecutionVertex vertex = groupVertex.getGroupMember(i);
				if (this.splitMap.put(vertex.getID(), new ArrayList<InputSplit>()) != null) {
					LOG.error("InputSplitTracker must keep track of two vertices with ID " + vertex.getID());
				}
			}
		}
	}

	/**
	 * Unregisters a job from the input split tracker.
	 * 
	 * @param eg
	 *        the execution graph of the job to be unregistered
	 */
	void unregisterJob(final ExecutionGraph eg) {

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
		while (it.hasNext()) {
			this.splitMap.remove(it.next().getID());
		}
	}

	/**
	 * Returns the input split with the given sequence number from the specified vertex's log or <code>null</code> if no
	 * such input split exists.
	 * 
	 * @param vertex
	 *        the vertex for which the input split shall be returned from the log
	 * @param sequenceNumber
	 *        the sequence number identifying the log entry
	 * @return the input split that was stored under the given sequence number of the vertex's log or <code>null</code>
	 *         if no such input split exists
	 */
	InputSplit getInputSplitFromLog(final ExecutionVertex vertex, final int sequenceNumber) {

		final List<InputSplit> inputSplitLog = this.splitMap.get(vertex.getID());
		if (inputSplitLog == null) {
			LOG.error("Cannot find input split log for vertex " + vertex + " (" + vertex.getID() + ")");
			return null;
		}

		synchronized (inputSplitLog) {

			if (sequenceNumber < inputSplitLog.size()) {
				return inputSplitLog.get(sequenceNumber);
			}
		}

		return null;
	}

	/**
	 * Adds the given input split to the vertex's log and stores it under the specified sequence number.
	 * 
	 * @param vertex
	 *        the vertex for which the input split shall be stored
	 * @param sequenceNumber
	 *        the sequence number identifying the log entry under which the input split shall be stored
	 * @param inputSplit
	 *        the input split to be stored
	 */
	void addInputSplitToLog(final ExecutionVertex vertex, final int sequenceNumber, final InputSplit inputSplit) {

		final List<InputSplit> inputSplitLog = this.splitMap.get(vertex.getID());
		if (inputSplitLog == null) {
			LOG.error("Cannot find input split log for vertex " + vertex + " (" + vertex.getID() + ")");
			return;
		}

		synchronized (inputSplitLog) {
			if (inputSplitLog.size() != sequenceNumber) {
				LOG.error("Expected input split with sequence number " + inputSplitLog.size() + " for vertex " + vertex
					+ " (" + vertex.getID() + ") but received " + sequenceNumber + ", skipping...");
				return;
			}

			inputSplitLog.add(inputSplit);
		}
	}
}
