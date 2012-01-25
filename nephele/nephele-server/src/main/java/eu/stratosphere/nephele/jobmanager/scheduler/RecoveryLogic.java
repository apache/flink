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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.CheckpointReplayResult;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.util.SerializableArrayList;
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

		LOG.info("Starting recovery for failed vertex " + failedVertex);

		final Set<ExecutionVertex> verticesToBeCanceled = new HashSet<ExecutionVertex>();

		final Map<AbstractInstance, List<ExecutionVertexID>> checkpointsToBeReplayed = new HashMap<AbstractInstance, List<ExecutionVertexID>>();

		findVerticesToRestart(failedVertex, verticesToBeCanceled, checkpointsToBeReplayed);

		// Restart all predecessors without checkpoint
		final Iterator<ExecutionVertex> cancelIterator = verticesToBeCanceled.iterator();
		while (cancelIterator.hasNext()) {

			final ExecutionVertex vertex = cancelIterator.next();
			LOG.info(vertex + " is canceled by recovery logic");
			final TaskCancelResult cancelResult = vertex.cancelTask();
			verticesToBeRestarted.put(vertex.getID(), vertex);
			if (cancelResult.getReturnCode() != ReturnCode.SUCCESS) {
				verticesToBeRestarted.remove(vertex.getID());
				LOG.error(cancelResult.getDescription());
				return false;
			}

		}

		// Replay all necessary checkpoints
		final Iterator<Map.Entry<AbstractInstance, List<ExecutionVertexID>>> checkpointIterator = checkpointsToBeReplayed
			.entrySet().iterator();

		while (checkpointIterator.hasNext()) {

			final Map.Entry<AbstractInstance, List<ExecutionVertexID>> entry = checkpointIterator.next();
			final AbstractInstance instance = entry.getKey();

			try {
				final List<CheckpointReplayResult> results = instance.replayCheckpoints(entry.getValue());
				for (final CheckpointReplayResult result : results) {
					if (result.getReturnCode() != ReturnCode.SUCCESS) {
						LOG.error(result.getDescription());
						return false;
					}
				}

			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
				return false;
			}
		}

		// Restart failed vertex
		if (failedVertex.getAllocatedResource().getInstance() instanceof DummyInstance) {
			failedVertex.updateExecutionState(ExecutionState.CREATED);
		} else {
			failedVertex.updateExecutionState(ExecutionState.ASSIGNED);
		}

		return true;
	}

	private static void findVerticesToRestart(final ExecutionVertex failedVertex,
			final Set<ExecutionVertex> verticesToBeCanceled,
			final Map<AbstractInstance, List<ExecutionVertexID>> checkpointsToBeReplayed) {

		final Queue<ExecutionVertex> verticesToTest = new ArrayDeque<ExecutionVertex>();
		final Set<ExecutionVertex> visited = new HashSet<ExecutionVertex>();
		verticesToTest.add(failedVertex);

		while (!verticesToTest.isEmpty()) {

			final ExecutionVertex vertex = verticesToTest.poll();

			if (!vertex.getID().equals(failedVertex.getID())) {
				verticesToBeCanceled.add(vertex);
			}

			// Predecessors must be either checkpoints or need to be restarted, too
			for (int j = 0; j < vertex.getNumberOfPredecessors(); j++) {
				final ExecutionVertex predecessor = vertex.getPredecessor(j);
				if (predecessor.getCheckpointState() != CheckpointState.PARTIAL
						&& predecessor.getCheckpointState() != CheckpointState.COMPLETE) {

					verticesToBeCanceled.add(predecessor);
					if (!visited.contains(predecessor)) {
						verticesToTest.add(predecessor);
					}
				} else {

					// Group IDs by instance
					final AbstractInstance instance = predecessor.getAllocatedResource().getInstance();
					List<ExecutionVertexID> checkpointIDs = checkpointsToBeReplayed.get(instance);
					if (checkpointIDs == null) {
						checkpointIDs = new SerializableArrayList<ExecutionVertexID>();
						checkpointsToBeReplayed.put(instance, checkpointIDs);
					}

					if (!checkpointIDs.contains(predecessor.getID())) {
						checkpointIDs.add(predecessor.getID());
					}
				}
			}
			visited.add(vertex);
		}
	}
}
