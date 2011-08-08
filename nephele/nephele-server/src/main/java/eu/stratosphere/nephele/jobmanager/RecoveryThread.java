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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.discovery.DiscoveryException;
import eu.stratosphere.nephele.discovery.DiscoveryService;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;
/**
 * @author marrus
 *
 */
public class RecoveryThread extends Thread {
	
	private static final Log LOG = LogFactory.getLog(RecoveryThread.class);
	final ExecutionGraph job;
	final List<ExecutionVertex> failedVertices;
	private List<ExecutionVertex> checkpoints;
	final private JobManagerProtocol jobManager;
	private List<ExecutionVertex>  globalConsistentCheckpoint = new ArrayList<ExecutionVertex>();
	
	
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
		this.failedVertices = job.getFailedVertices();
		this.checkpoints = job.getVerticesWithCheckpoints();
		LOG.info("RecoveryThread");
	
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {
		LOG.info("recovery running");
		if(this.failedVertices.isEmpty()){
			LOG.error("No failed vertices to recover" );
		}
		Iterator<ExecutionVertex> vertexIter = failedVertices.iterator();
		//Change InputChannels for every Vertex
		while(vertexIter.hasNext()){
			
			//collect incoming channels
			ArrayList<AbstractInputChannel> channels = new ArrayList<AbstractInputChannel>();
			ExecutionVertex failed = vertexIter.next();
			List<ExecutionVertex> restart = findRestarts(failed);
			for (int j = 0; j < failed.getEnvironment().getNumberOfInputGates(); j++) {
				InputGate<? extends Record> ingate = failed.getEnvironment().getInputGate(j);
				for (int k = 0; k < ingate.getNumberOfInputChannels(); k++) {
					 AbstractInputChannel<? extends Record> channel = ingate.getInputChannel(k);
					 channel.releaseResources();
					 channels.add(channel);
					
				}
			}
			for (int i = 0; i < failed.getNumberOfPredecessors(); i++) {
				ExecutionVertex predecessor = failed.getPredecessor(i);
				
				if(this.globalConsistentCheckpoint.contains(predecessor)){
					for (int j = 0; j < predecessor.getEnvironment().getNumberOfOutputGates(); j++) {
					OutputGate<? extends Record> outgate = predecessor.getEnvironment().getOutputGate(j);
					for (int k = 0; k < outgate.getNumberOfOutputChannels(); k++) {
						 AbstractOutputChannel<? extends Record> channel = outgate.getOutputChannel(k);
						 if(channels.contains(channel.getConnectedChannelID())){
							 predecessor.getAllocatedResource().getInstance().recover(channel.getID());
						 }
						
					}
				}
						
				}

				
			}
			Iterator<ExecutionVertex> restartIterator = restart.iterator();
			
			LOG.info("Checkpoints are");
			for(int k = 0; k< this.globalConsistentCheckpoint.size();k++){
				LOG.info(this.globalConsistentCheckpoint.get(k).getName());
			}
		
			Iterator<ExecutionVertex> checkpointIterator = this.globalConsistentCheckpoint.iterator();
			while(checkpointIterator.hasNext()){
				ExecutionVertex checkpoint = checkpointIterator.next();
//				for (int i = 0; i < checkpoint.getNumberOfSuccessors(); i++) {
//					ExecutionVertex vertex = checkpoint.getSuccessor(i);
//					Environment ee = vertex.getEnvironment();
//					for (int j = 0; j < ee.getNumberOfInputGates(); j++) {
//						InputGate<? extends Record> ingate = ee.getInputGate(j);
//						
//						for (int k = 0; k < ingate.getNumberOfInputChannels(); k++) {
//							ingate.replaceChannel(ingate.getInputChannel(k).getID(), ChannelType.FILE);
//
//						}
//						
//						
//					}
//				}
				
				AbstractInstance instance = checkpoint.getAllocatedResource().getInstance();

				instance.recoverAll(checkpoint.getEnvironment().getOutputGate(0).getOutputChannel(0).getID());
			}
			while(restartIterator.hasNext()){
				ExecutionVertex vertex = restartIterator.next();
				LOG.info("'Test Restarting " + vertex.getName() );
				if(!vertex.equals(failed)){
				LOG.info("Restarting " + vertex.getName() );
				
				vertex.getEnvironment().restartExecution();
				
				}
			}
		}
		
		this.job.executionStateChanged(null, ExecutionState.RERUNNING, null);
	}


	/**
	 * @param failed 
	 * @return
	 */
	private List<ExecutionVertex> findRestarts(ExecutionVertex failed) {
		ArrayList<ExecutionVertex> restart = new ArrayList<ExecutionVertex>();
		Queue<ExecutionVertex> totest = new PriorityQueue<ExecutionVertex>();
		ArrayList<ExecutionVertex> visited = new ArrayList<ExecutionVertex>();
		totest.add(failed);
		
		
		while(!totest.isEmpty()){
			//Add all followers
			ExecutionVertex vertex = totest.poll();
			
			if(!restart.contains(vertex)){
				restart.add(vertex);
			}
			for(int i = 0; i < vertex.getNumberOfSuccessors(); i++){
				ExecutionVertex successor = vertex.getSuccessor(i);
				//totest.add(successor);
				if(successor.isCheckpoint()){
					this.checkpoints.remove(successor);
				}
				System.out.println("add " + successor.getName() + " torestart");
				List<ExecutionVertex> follower = findFollowers(successor, restart);
				restart.addAll(follower);
				Iterator<ExecutionVertex> iter = follower.iterator();
				while(iter.hasNext()){
					ExecutionVertex follow = iter.next();
					if(!visited.contains(follow)){
						totest.add(follow);
					}
				}
			}
			for(int j = 0; j < vertex.getNumberOfPredecessors(); j++){
				ExecutionVertex predecessor = vertex.getPredecessor(j);
				if(!predecessor.isCheckpoint()){
					System.out.println("add " + predecessor.getName() + " torestart");

					restart.add(predecessor);
					if(!visited.contains(predecessor)){
						totest.add(predecessor);
					}
				}else{
					if(!this.globalConsistentCheckpoint.contains(predecessor)){
					this.globalConsistentCheckpoint.add(predecessor);
					}
					System.out.println(predecessor.getName() + " is checkpoint");
					List<ExecutionVertex> follower = findFollowers(predecessor, restart);
					for (int i = 0; i < follower.size(); i++) {
						System.out.println("add " + follower.get(i) + " torestart");
					}
					restart.addAll(follower);
					Iterator<ExecutionVertex> iter = follower.iterator();
					while(iter.hasNext()){
						ExecutionVertex follow = iter.next();
						if(!visited.contains(follow)){
							totest.add(follow);
						}
					}
					
				}
			}
			visited.add(vertex);
		}
		
		
		
		
		return restart;
	}
	private List<ExecutionVertex> findFollowers(ExecutionVertex vertex,ArrayList<ExecutionVertex> restart ){
		ArrayList<ExecutionVertex> follower = new ArrayList<ExecutionVertex>();

			for (int i = 0; i < vertex.getNumberOfSuccessors(); i++){
				ExecutionVertex successor = vertex.getSuccessor(i);
				if(!restart.contains(successor)){
					follower.add(successor);
					if(successor.isCheckpoint()){
						this.checkpoints.remove(successor);
						//TODO(marrus) remove File
						successor.discardCheckpoint();
					}
				}
			}
		
		return follower;
	}
	
}
