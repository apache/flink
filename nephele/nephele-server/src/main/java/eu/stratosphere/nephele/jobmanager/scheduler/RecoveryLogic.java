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
package eu.stratosphere.nephele.jobmanager.scheduler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.SerializableHashSet;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * @author marrus
 */
public final class RecoveryLogic {

	/**
	 * The logger to report information and problems.
	 */
	private static final Log LOG = LogFactory.getLog(RecoveryLogic.class);

	/**
	 * Private constructor so class cannot be instantiated.
	 */
	private RecoveryLogic() {
	}

	public static boolean recover(final ExecutionVertex failedVertex,
			final Map<ExecutionVertexID, ExecutionVertex> verticesToBeRestarted) {

		// Perform initial sanity check
		if (failedVertex.getExecutionState() != ExecutionState.FAILED) {
			LOG.error("Vertex " + failedVertex + " is requested to be recovered, but is not failed");
			return false;
		}

		final ExecutionGraph eg = failedVertex.getExecutionGraph();
		synchronized (eg) {

			LOG.info("Starting recovery for failed vertex " + failedVertex);

			final Set<ExecutionVertex> verticesToBeCanceled = new HashSet<ExecutionVertex>();

			final Set<ExecutionVertex> checkpointsToBeReplayed = new HashSet<ExecutionVertex>();

			findVerticesToRestart(failedVertex, verticesToBeCanceled, checkpointsToBeReplayed);

			// Restart all predecessors without checkpoint
			final Iterator<ExecutionVertex> cancelIterator = verticesToBeCanceled.iterator();
			while (cancelIterator.hasNext()) {

				final ExecutionVertex vertex = cancelIterator.next();

				if (vertex.compareAndUpdateExecutionState(ExecutionState.FINISHED, getStateToUpdate(vertex))) {
					LOG.info("Vertex " + vertex + " has already finished and will not be canceled");
					continue;
				}

				LOG.info(vertex + " is canceled by recovery logic");
				verticesToBeRestarted.put(vertex.getID(), vertex);
				final TaskCancelResult cancelResult = vertex.cancelTask();

				if (cancelResult.getReturnCode() != ReturnCode.SUCCESS
						&& cancelResult.getReturnCode() != ReturnCode.TASK_NOT_FOUND) {

					verticesToBeRestarted.remove(vertex.getID());
					LOG.error(cancelResult.getDescription());
					return false;
				}
			}

			LOG.info("Starting cache invalidation");

			// Invalidate the lookup caches
			if (!invalidateReceiverLookupCaches(failedVertex, verticesToBeCanceled)) {
				return false;
			}

			LOG.info("Cache invalidation complete");

			// Replay all necessary checkpoints
			final Iterator<ExecutionVertex> checkpointIterator = checkpointsToBeReplayed.iterator();

			while (checkpointIterator.hasNext()) {

				checkpointIterator.next().updateExecutionState(ExecutionState.ASSIGNED);
			}

			// Restart failed vertex
			failedVertex.updateExecutionState(getStateToUpdate(failedVertex));
		}

		return true;
	}

	private static ExecutionState getStateToUpdate(final ExecutionVertex vertex) {

		if (vertex.getAllocatedResource().getInstance() instanceof DummyInstance) {
			return ExecutionState.CREATED;
		}

		return ExecutionState.ASSIGNED;
	}

	private static void findVerticesToRestart(final ExecutionVertex failedVertex,
			final Set<ExecutionVertex> verticesToBeCanceled,
			final Set<ExecutionVertex> checkpointsToBeReplayed) {

		final Queue<ExecutionVertex> verticesToTest = new ArrayDeque<ExecutionVertex>();
		final Set<ExecutionVertex> visited = new HashSet<ExecutionVertex>();
		verticesToTest.add(failedVertex);

		System.out.println("++++" + failedVertex + " failed");

		while (!verticesToTest.isEmpty()) {

			final ExecutionVertex vertex = verticesToTest.poll();

			if (!vertex.getID().equals(failedVertex.getID())) {
				verticesToBeCanceled.add(vertex);
			}

			// Predecessors must be either checkpoints or need to be restarted, too
			for (int j = 0; j < vertex.getNumberOfPredecessors(); j++) {
				final ExecutionVertex predecessor = vertex.getPredecessor(j);
				System.out.println("++++ Predecessor " + predecessor + " has checkpoint state "
					+ predecessor.getCheckpointState());
				if (predecessor.getCheckpointState() != CheckpointState.PARTIAL
						&& predecessor.getCheckpointState() != CheckpointState.COMPLETE) {

					verticesToBeCanceled.add(predecessor);
					if (!visited.contains(predecessor)) {
						verticesToTest.add(predecessor);
					}
				} else {
					checkpointsToBeReplayed.add(predecessor);
				}
			}
			visited.add(vertex);
		}
	}

	private static final boolean invalidateReceiverLookupCaches(final ExecutionVertex failedVertex,
			final Set<ExecutionVertex> verticesToBeCanceled) {

		final Map<AbstractInstance, Set<ChannelID>> entriesToInvalidate = new HashMap<AbstractInstance, Set<ChannelID>>();

		collectCacheEntriesToInvalidate(failedVertex, entriesToInvalidate);
		for (final Iterator<ExecutionVertex> it = verticesToBeCanceled.iterator(); it.hasNext();) {
			collectCacheEntriesToInvalidate(it.next(), entriesToInvalidate);
		}

		final Iterator<Map.Entry<AbstractInstance, Set<ChannelID>>> it = entriesToInvalidate.entrySet().iterator();

		while (it.hasNext()) {

			final Map.Entry<AbstractInstance, Set<ChannelID>> entry = it.next();
			final AbstractInstance instance = entry.getKey();

			try {
				instance.invalidateLookupCacheEntries(entry.getValue());
			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
				return false;
			}
		}

		return true;
	}

	private static void collectCacheEntriesToInvalidate(final ExecutionVertex vertex,
			final Map<AbstractInstance, Set<ChannelID>> entriesToInvalidate) {

		final ExecutionGraph eg = vertex.getExecutionGraph();

		final RuntimeEnvironment env = vertex.getEnvironment();
		for (int i = 0; i < env.getNumberOfOutputGates(); ++i) {

			final OutputGate<? extends Record> outputGate = env.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); ++j) {

				final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);
				if (outputChannel.getType() == ChannelType.FILE) {
					// Connected vertex is not yet running
					continue;
				}

				final ChannelID connectedChannelID = outputChannel.getConnectedChannelID();
				final ExecutionVertex connectedVertex = eg.getVertexByChannelID(connectedChannelID);
				if (connectedVertex == null) {
					LOG.error("Connected vertex is null");
					continue;
				}

				final AbstractInstance instance = connectedVertex.getAllocatedResource().getInstance();
				Set<ChannelID> channelIDs = entriesToInvalidate.get(instance);
				if (channelIDs == null) {
					channelIDs = new SerializableHashSet<ChannelID>();
					entriesToInvalidate.put(instance, channelIDs);
				}

				channelIDs.add(connectedChannelID);
			}
		}

		for (int i = 0; i < env.getNumberOfInputGates(); ++i) {

			final InputGate<? extends Record> inputGate = env.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfInputChannels(); ++j) {

				final AbstractInputChannel<? extends Record> inputChannel = inputGate.getInputChannel(j);
				if (inputChannel.getType() == ChannelType.FILE) {
					// Connected vertex is not running anymore
					continue;
				}

				final ChannelID connectedChannelID = inputChannel.getConnectedChannelID();
				final ExecutionVertex connectedVertex = eg.getVertexByChannelID(connectedChannelID);
				if (connectedVertex == null) {
					LOG.error("Connected vertex is null");
					continue;
				}

				final AbstractInstance instance = connectedVertex.getAllocatedResource().getInstance();
				Set<ChannelID> channelIDs = entriesToInvalidate.get(instance);
				if (channelIDs == null) {
					channelIDs = new SerializableHashSet<ChannelID>();
					entriesToInvalidate.put(instance, channelIDs);
				}

				channelIDs.add(connectedChannelID);
			}
		}
	}
}
