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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * @author marrus
 */
public class RecoveryThread extends Thread {

	private static final Log LOG = LogFactory.getLog(RecoveryThread.class);

	final ExecutionGraph job;

	final List<ExecutionVertex> failedVertices;

	private List<ExecutionVertex> checkpoints;

	final private JobManagerProtocol jobManager;

	private List<ExecutionVertex> globalConsistentCheckpoint = new ArrayList<ExecutionVertex>();

	/**
	 * Initializes RecoveryThread.
	 * 
	 * @param job
	 * @param jobManager
	 * @throws Exception
	 */
	public RecoveryThread(ExecutionGraph job, JobManager jobManager) throws Exception {
		super();
		this.job = job;
		this.jobManager = jobManager;
		this.failedVertices = new ArrayList<ExecutionVertex>();
		this.failedVertices.addAll(job.getFailedVertices());
		this.checkpoints = job.getVerticesWithCheckpoints();
		LOG.info("RecoveryThread");

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {
		if (this.failedVertices.isEmpty()) {
			LOG.error("No failed vertices to recover");
		}
		Iterator<ExecutionVertex> vertexIter = this.failedVertices.iterator();

		while (vertexIter.hasNext()) {

			// ArrayList<AbstractInputChannel> channels = new ArrayList<AbstractInputChannel>();
			ExecutionVertex failed = vertexIter.next();

			LOG.info("Staring Recovery for " + failed);
			List<ExecutionVertex> restart = findRestarts(failed);

			Iterator<ExecutionVertex> restartIterator = restart.iterator();
			while (restartIterator.hasNext()) {
				ExecutionVertex vertex = restartIterator.next();

				if (!vertex.equals(failed)) {
					LOG.info("Restarting " + vertex.getName());
					final List<ExecutionVertexID> checkpointsToReplay = new ArrayList<ExecutionVertexID>();
					checkpointsToReplay.add(vertex.getID());

					try {
						vertex.getAllocatedResource().getInstance().replayCheckpoints(checkpointsToReplay);
					} catch (Exception e) {
						LOG.info("Catched Exception " + StringUtils.stringifyException(e) + "wihle restarting");
					}
				}

			}

			//TODO: I don't think this code is needed anymore (DW)
			/*Iterator<ExecutionVertex> checkpointIterator = this.globalConsistentCheckpoint.iterator();
			while (checkpointIterator.hasNext()) {
				ExecutionVertex checkpoint = checkpointIterator.next();

				AbstractInstance instance = checkpoint.getAllocatedResource().getInstance();

				instance.recoverAll(checkpoint.getEnvironment().getOutputGate(0).getOutputChannel(0).getID());
			}*/

			LOG.info("Finished recovery for " + failed);
		}

		this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.RERUNNING, null);
		LOG.info("Recovery Finsihed");
	}

	/**
	 * @param failed
	 * @return
	 */
	private List<ExecutionVertex> findRestarts(ExecutionVertex failed) {
		LOG.info("in findRestarts");
		ArrayList<ExecutionVertex> restart = new ArrayList<ExecutionVertex>();
		Queue<ExecutionVertex> totest = new ArrayDeque<ExecutionVertex>();
		ArrayList<ExecutionVertex> visited = new ArrayList<ExecutionVertex>();

		totest.add(failed);
		int k = 0;
		LOG.info("added totest");
		ExecutionVertex vertex = failed;
		while (!totest.isEmpty()) {
			// Add all followers

			LOG.info("in while");
			if (k != 0) {
				vertex = totest.peek();
			}
			LOG.info("Testing " + vertex.getName());
			k++;
			totest.remove(vertex);
			if (!restart.contains(vertex)) {
				restart.add(vertex);
			}
			for (int i = 0; i < vertex.getNumberOfSuccessors(); i++) {
				ExecutionVertex successor = vertex.getSuccessor(i);
				restart.add(successor);
				LOG.info("add " + successor.getName() + " torestart");
				// totest.add(successor);
				if (successor.getCheckpointState() == CheckpointState.PARTIAL) {
					this.checkpoints.remove(successor);
				}

				List<ExecutionVertex> follower = findFollowers(successor, restart);
				restart.addAll(follower);
				Iterator<ExecutionVertex> iter = follower.iterator();
				while (iter.hasNext()) {
					ExecutionVertex follow = iter.next();
					if (!visited.contains(follow)) {
						LOG.info("add totest" + follow.getName());
						totest.add(follow);
					}
				}
			}
			for (int j = 0; j < vertex.getNumberOfPredecessors(); j++) {
				ExecutionVertex predecessor = vertex.getPredecessor(j);
				if (predecessor.getCheckpointState() != CheckpointState.PARTIAL) {
					LOG.info("add " + predecessor.getName() + " torestart");

					restart.add(predecessor);
					if (!visited.contains(predecessor)) {
						totest.add(predecessor);
						LOG.info("add totest" + predecessor);
					}
				} else {
					if (!this.globalConsistentCheckpoint.contains(predecessor)) {
						this.globalConsistentCheckpoint.add(predecessor);
					}

					List<ExecutionVertex> follower = findFollowers(predecessor, restart);
					for (int i = 0; i < follower.size(); i++) {
						LOG.info("add " + follower.get(i) + " torestart");
					}
					restart.addAll(follower);
					Iterator<ExecutionVertex> iter = follower.iterator();
					while (iter.hasNext()) {
						ExecutionVertex follow = iter.next();
						if (!visited.contains(follow)) {
							LOG.info("add totest" + follow.getName());
							totest.add(follow);
						}
					}

				}
			}
			visited.add(vertex);
		}

		LOG.info("finderestartsfinished");

		return restart;
	}

	private List<ExecutionVertex> findFollowers(ExecutionVertex vertex, ArrayList<ExecutionVertex> restart) {
		ArrayList<ExecutionVertex> follower = new ArrayList<ExecutionVertex>();

		for (int i = 0; i < vertex.getNumberOfSuccessors(); i++) {
			ExecutionVertex successor = vertex.getSuccessor(i);
			if (!restart.contains(successor)) {
				follower.add(successor);
				if (successor.getCheckpointState() == CheckpointState.PARTIAL) {
					this.checkpoints.remove(successor);
					// TODO(marrus) remove File
					final List<ExecutionVertexID> checkpointsToRemove = new ArrayList<ExecutionVertexID>();
					checkpointsToRemove.add(successor.getID());
					try {
						successor.getAllocatedResource().getInstance().removeCheckpoints(checkpointsToRemove);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		return follower;
	}

}