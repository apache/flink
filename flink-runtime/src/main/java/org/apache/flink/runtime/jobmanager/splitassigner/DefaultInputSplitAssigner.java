/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.jobmanager.splitassigner;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionGroupVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This is the default implementation of the {@link InputSplitAssigner} interface. The default input split assigner
 * simply returns all input splits of an input vertex in the order they were originally computed. The default input
 * split assigner is always used when a more specific {@link InputSplitAssigned} could not be found.
 * <p>
 * This class is thread-safe.
 * 
 */
public class DefaultInputSplitAssigner implements InputSplitAssigner {

	/**
	 * The logging object used to report information and errors.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultInputSplitAssigner.class);

	/**
	 * The split map stores a list of all input splits that still must be consumed by a specific input vertex.
	 */
	private final ConcurrentMap<ExecutionGroupVertex, Queue<InputSplit>> splitMap = new ConcurrentHashMap<ExecutionGroupVertex, Queue<InputSplit>>();


	@Override
	public void registerGroupVertex(final ExecutionGroupVertex groupVertex) {

		final InputSplit[] inputSplits = groupVertex.getInputSplits();

		if (inputSplits == null) {
			return;
		}

		if (inputSplits.length == 0) {
			return;
		}

		final Queue<InputSplit> queue = new ConcurrentLinkedQueue<InputSplit>();
		if (this.splitMap.putIfAbsent(groupVertex, queue) != null) {
			LOG.error("Group vertex " + groupVertex.getName() + " already has a split queue");
		}

		queue.addAll(Arrays.asList(inputSplits));
	}


	@Override
	public void unregisterGroupVertex(final ExecutionGroupVertex groupVertex) {

		this.splitMap.remove(groupVertex);
	}


	@Override
	public InputSplit getNextInputSplit(final ExecutionVertex vertex) {

		final Queue<InputSplit> queue = this.splitMap.get(vertex.getGroupVertex());
		if (queue == null) {
			final JobID jobID = vertex.getExecutionGraph().getJobID();
			LOG.error("Cannot find split queue for vertex " + vertex.getGroupVertex() + " (job " + jobID + ")");
			return null;
		}

		InputSplit nextSplit = queue.poll();

		if (LOG.isDebugEnabled() && nextSplit != null) {
			LOG.debug("Assigning split " + nextSplit.getSplitNumber() + " to " + vertex);
		}

		return nextSplit;
	}
}
