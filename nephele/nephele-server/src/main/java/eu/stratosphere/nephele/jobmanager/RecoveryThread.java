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
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.util.SerializableArrayList;
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

	private List<ExecutionVertexID> globalConsistentCheckpoint = new SerializableArrayList<ExecutionVertexID>();

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
		List<CheckpointReplayResult> replayCheckpoints = new ArrayList();
		Iterator<ExecutionVertex> vertexIter = this.failedVertices.iterator();

		while (vertexIter.hasNext()) {

			// ArrayList<AbstractInputChannel> channels = new ArrayList<AbstractInputChannel>();
			ExecutionVertex failed = vertexIter.next();

			LOG.info("Staring Recovery for " + failed);
			//findRestarts(failed);
			
			List<ExecutionVertex> restart = findRestarts(failed);
			
			//restart all predecessors without checkpoint
			Iterator<ExecutionVertex> restartIterator = restart.iterator();
			while (restartIterator.hasNext()) {
				ExecutionVertex vertex = restartIterator.next();
				if(vertex.getID() != failed.getID()){
					try {
						vertex.getAllocatedResource().getInstance().restartTask(vertex.getID(),this.job.getJobConfiguration(), vertex.getEnvironment(), vertex.constructInitialActiveOutputChannelsSet() );
					} catch (IOException e) {
						e.printStackTrace();
						this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
						return;
					}
				}

			}
			//restart failed vertex
			try {
				failed.getAllocatedResource().getInstance().submitTask(failed.getID(), this.job.getJobConfiguration(), failed.getEnvironment(), failed.constructInitialActiveOutputChannelsSet());
			} catch (IOException e1) {
				e1.printStackTrace();
				this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
				return;
			}

			//get list of instances of consistencheckpoints
			List<AbstractInstance> instances = new SerializableArrayList<AbstractInstance>();
			for(ExecutionVertexID id : this.globalConsistentCheckpoint){
				AbstractInstance instance = this.job.getVertexByID(id).getAllocatedResource().getInstance();
				if(!instances.contains(instance)){
					instances.add(instance);
				}
			}
			Iterator<AbstractInstance> instanceIterator = instances.iterator();
			while(instanceIterator.hasNext()){
				//replay all necessary checkpoints
				try {

					replayCheckpoints.addAll(instanceIterator.next().replayCheckpoints(this.globalConsistentCheckpoint));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		}
		for(CheckpointReplayResult replayResult : replayCheckpoints ){
			if(replayResult.getReturnCode() == ReturnCode.ERROR){
				LOG.info("Replay of Checkpoints return Error " + replayResult.getDescription() );
				this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.FAILED, null);
				return;
			}
		}
		
		this.job.executionStateChanged(this.job.getJobID(), null, ExecutionState.RERUNNING, null);
		LOG.info("Recovery Finished");
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
//			for (int i = 0; i < vertex.getNumberOfSuccessors(); i++) {
//				//all successors must be restarted 
//				ExecutionVertex successor = vertex.getSuccessor(i);
//				restart.add(successor);
//				if (successor.getCheckpointState() == CheckpointState.PARTIAL) {
//					//these tasks will be restarted, delete checkpoints
//					this.checkpoints.remove(successor);
//					this.globalConsistentCheckpoint.remove(successor.getID());
//				}
//				//all followers must be restarted 
//				List<ExecutionVertex> follower = findFollowers(successor, restart);
//				restart.addAll(follower);
//				Iterator<ExecutionVertex> iter = follower.iterator();
//				while (iter.hasNext()) {
//					ExecutionVertex follow = iter.next();
//					if (!visited.contains(follow)) {
//						totest.add(follow);
//					}
//				}
//			}
			//predecessors must be either checkpoints or need to be restarted too
			for (int j = 0; j < vertex.getNumberOfPredecessors(); j++) {
				ExecutionVertex predecessor = vertex.getPredecessor(j);
				if (predecessor.getCheckpointState() != CheckpointState.PARTIAL) {

					restart.add(predecessor);
					if (!visited.contains(predecessor)) {
						totest.add(predecessor);
					}
				} else {
					if (!this.globalConsistentCheckpoint.contains(predecessor.getID())) {
						this.globalConsistentCheckpoint.add(predecessor.getID());
					}

					
//					List<ExecutionVertex> follower = findFollowers(predecessor, restart);
//					restart.addAll(follower);
//					Iterator<ExecutionVertex> iter = follower.iterator();
//					while (iter.hasNext()) {
//						ExecutionVertex follow = iter.next();
//						if (!visited.contains(follow)) {
//							totest.add(follow);
//						}
//					}

				}
			}
			visited.add(vertex);
		}

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
					
					this.globalConsistentCheckpoint.remove(successor.getID());
					final SerializableArrayList<ExecutionVertexID> checkpointsToRemove = new SerializableArrayList<ExecutionVertexID>();
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