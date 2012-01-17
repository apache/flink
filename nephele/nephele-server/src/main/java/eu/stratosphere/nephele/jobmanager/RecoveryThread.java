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

import eu.stratosphere.nephele.checkpointing.CheckpointReplayResult;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 * @author marrus
 */
public class RecoveryThread extends Thread {

	private static final Log LOG = LogFactory.getLog(RecoveryThread.class);

	private final ExecutionGraph job;

	List<ExecutionVertex> failedVertices;
	
	List<ExecutionVertex> recovered = new ArrayList<ExecutionVertex>();
	private List<ExecutionVertexID> globalConsistentCheckpoint = new SerializableArrayList<ExecutionVertexID>();

	/**
	 * Initializes RecoveryThread.
	 * 
	 * @param job The Job with 
	 * @throws Exception
	 */
	public RecoveryThread(final ExecutionGraph job) throws Exception {
		super("Recovery Thread");
		this.job = job;
		this.failedVertices = new ArrayList<ExecutionVertex>();
		this.failedVertices.addAll(job.getFailedVertices());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {
		if (this.failedVertices.isEmpty()) {
			LOG.error("No failed vertices to recover");
		}
		//FIXME (marrus) dirty fix
		while (!this.failedVertices.isEmpty()) { 
			ExecutionVertex torecover = this.failedVertices.remove(0);
			if(!this.recovered.contains(torecover)){
				recover(torecover);
				this.recovered.add(torecover);
			}
			if (this.failedVertices.isEmpty()) { 
				this.failedVertices = this.job.getFailedVertices();
			}
		}
		LOG.info("Recovery Finished");
	}

	/**
	 * 
	 */
	private boolean recover(final ExecutionVertex  failed) {
		List<CheckpointReplayResult> replayCheckpoints = new ArrayList<CheckpointReplayResult>();


			LOG.info("Staring Recovery for " + failed);
			//findRestarts(failed);
			
			final List<ExecutionVertex> restart = findRestarts(failed);
			
			//restart all predecessors without checkpoint
			Iterator<ExecutionVertex> restartIterator = restart.iterator();
			while (restartIterator.hasNext()) {
				ExecutionVertex vertex = restartIterator.next();
				if (vertex.getID() != failed.getID()) {
					try {
						vertex.getAllocatedResource().getInstance().restartTask(vertex.getID(),this.job.getJobConfiguration(), vertex.getEnvironment(), vertex.constructInitialActiveOutputChannelsSet() );
					} catch (IOException e) {
						e.printStackTrace();
						this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
						return false;
					}
				}

			}
			//restart failed vertex
			try {
				failed.getAllocatedResource().getInstance().submitTask(failed.getID(), this.job.getJobConfiguration(), failed.getEnvironment(), failed.constructInitialActiveOutputChannelsSet());
			} catch (IOException e1) {
				e1.printStackTrace();
				this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
				return false;
			}

			//get list of instances of consistent checkpoints
			List<AbstractInstance> instances = new SerializableArrayList<AbstractInstance>();
			for (ExecutionVertexID id : this.globalConsistentCheckpoint) {
				AbstractInstance instance = this.job.getVertexByID(id).getAllocatedResource().getInstance();
				if (!instances.contains(instance)) {
					instances.add(instance);
				}
			}
			Iterator<AbstractInstance> instanceIterator = instances.iterator();
			while (instanceIterator.hasNext()) {
				//replay all necessary checkpoints
				try {
					AbstractInstance instance = instanceIterator.next();
				
					replayCheckpoints.addAll(instance.replayCheckpoints(this.globalConsistentCheckpoint));
				} catch (IOException e) {
					e.printStackTrace();
					this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
					return false;
				}
			}
			//State job to Failed if a checkpoint-replay failed
			for(CheckpointReplayResult replayResult : replayCheckpoints ){
				if (replayResult.getReturnCode() == ReturnCode.ERROR) {
					LOG.info("Replay of Checkpoints return Error " + replayResult.getDescription() );
					this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
					return false;
				}
			}
			LOG.info("FINISHED RECOVERY for " + failed.getName());
			this.job.executionStateChanged(this.job.getJobID(), failed.getID(), ExecutionState.RERUNNING, null);

		return true;
	}

	/**
	 * @param failed
	 * @return
	 */
	private List<ExecutionVertex> findRestarts(ExecutionVertex failed) {
		ArrayList<ExecutionVertex> restart = new ArrayList<ExecutionVertex>();
		Queue<ExecutionVertex> totest = new ArrayDeque<ExecutionVertex>();
		ArrayList<ExecutionVertex> visited = new ArrayList<ExecutionVertex>();
		totest.add(failed);
		ExecutionVertex vertex = failed;
		while (!totest.isEmpty()) {
			
			vertex = totest.peek();
			totest.remove(vertex);
			if (!restart.contains(vertex)) {
				restart.add(vertex);
			}
			//predecessors must be either checkpoints or need to be restarted too
			for (int j = 0; j < vertex.getNumberOfPredecessors(); j++) {
				ExecutionVertex predecessor = vertex.getPredecessor(j);
				if (predecessor.getCheckpointState() != CheckpointState.PARTIAL 
						&&  predecessor.getCheckpointState() != CheckpointState.COMPLETE) {

					restart.add(predecessor);
					if (!visited.contains(predecessor)) {
						totest.add(predecessor);
					}
				} else {
					if (!this.globalConsistentCheckpoint.contains(predecessor.getID())) {
						this.globalConsistentCheckpoint.add(predecessor.getID());
					}


				}
			}
			visited.add(vertex);
		}

		return restart;
	}

//	private List<ExecutionVertex> findFollowers(ExecutionVertex vertex, ArrayList<ExecutionVertex> restart) {
//		ArrayList<ExecutionVertex> follower = new ArrayList<ExecutionVertex>();
//
//		for (int i = 0; i < vertex.getNumberOfSuccessors(); i++) {
//			ExecutionVertex successor = vertex.getSuccessor(i);
//			if (!restart.contains(successor)) {
//				follower.add(successor);
//				if (successor.getCheckpointState() == CheckpointState.PARTIAL) {
//					this.checkpoints.remove(successor);
//					
//					this.globalConsistentCheckpoint.remove(successor.getID());
//					final SerializableArrayList<ExecutionVertexID> checkpointsToRemove = new SerializableArrayList<ExecutionVertexID>();
//					checkpointsToRemove.add(successor.getID());
//					try {
//						successor.getAllocatedResource().getInstance().removeCheckpoints(checkpointsToRemove);
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//			}
//		}
//
//		return follower;
//	}

} 
