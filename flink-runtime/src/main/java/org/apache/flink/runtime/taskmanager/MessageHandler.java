/*
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

package org.apache.flink.runtime.taskmanager;


import org.apache.flink.api.common.messages.TaskMessage;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;

/**
 * Message handler for all tasks running under a particular {@link TaskManager}
 *
 * {@link TaskManager} has access to this Message Handler and uses it to send and receive message to and
 * from all tasks running under this {@link TaskManager}
 */
public class MessageHandler{
	/** List of all {@link TaskMessageHandler}s*/
	private HashMap<JobVertexID, TaskMessageHandler> handlers;
	private ActorGateway gateway;
	private boolean isShutDown;
	private int maxSize;

	/**
	 * Constructor for this message handler. Initially, everything is shut down.
	 * Called from {@link TaskManager}startTaskManagerComponentsAndActor(Configuration, ActorSystem, String, Option, Option, boolean, StreamingMode, Class)}
	 */
	MessageHandler(){
		this.gateway = null;
		this.handlers = null;
		this.isShutDown = true;
	}

	/**
	 * {@link TaskManager} sets this message handler up when it itself registers to a
	 * {@link org.apache.flink.runtime.jobmanager.JobManager}
	 *
	 * Called from {@link TaskManager}#associateWithJobManager(ActorRef, InstanceID, int, UUID)}
	 *
	 * @param gateway Gateway to the {@link TaskManager}
	 * @param maxSize Maximum size of message queue for each task
	 */
	void setup(ActorGateway gateway, int maxSize){
		this.handlers = new HashMap<JobVertexID, TaskMessageHandler>();
		this.gateway = gateway;
		this.isShutDown = false;
		this.maxSize = maxSize;
	}

	/**
	 * Called by {@link TaskManager} when it itself is shutting down for what-so-ever-reason.
	 *
	 */
	void shutDown(){
		synchronized (this){
			for(TaskMessageHandler handler: this.handlers.values()){
				handler.shutDown();
			}
			this.isShutDown = true;
			this.handlers.clear();
		}
	}

	/**
	 * Register a particular task with this message handler. If a handler already exists for a particular
	 * {@link JobVertexID}, then subscribe the corresponding {@link TaskMessageHandler} to serve this
	 * parallel instance also.
	 *
	 * Called from {@link TaskManager}#submitTask(TaskDeploymentDescriptor)}
	 *
	 * @param vertexID Job Vertex id of the task being registered
	 * @param subTaskIndex Sub task index of this task
	 * @return the {@link TaskMessageHandler} for this task
	 */
	TaskMessageHandler registerTask(JobVertexID vertexID, int subTaskIndex) {
		synchronized (this){
			error();
			if(handlers.containsKey(vertexID)){
				handlers.get(vertexID).subscribe(subTaskIndex);
			} else{
				handlers.put(vertexID, new TaskMessageHandler(maxSize, vertexID, gateway));
				handlers.get(vertexID).subscribe(subTaskIndex);
			}
			return handlers.get(vertexID);
		}
	}

	/**
	 * Unregister a particular task from listening and receiving any messages. If all parallel instances
	 * associated with a particular {@link JobVertexID} have unregistered, the handler for that
	 * {@link JobVertexID} will be shut down and cleared out of our handler lists.
	 *
	 * Called from {@link TaskManager}#unregisterTaskAndNotifyFinalState(ExecutionAttemptID)
	 *
	 * @param vertexID Job Vertex id of the task being unregistered
	 * @param subTaskIndex Sub task index of this task
	 */
	void unregisterTask(JobVertexID vertexID, int subTaskIndex){
		synchronized (this){
			error();
			if(handlers.containsKey(vertexID)){
				boolean unregister = handlers.get(vertexID).unSubscribe(subTaskIndex);
				if(unregister){
					handlers.get(vertexID).shutDown();
					handlers.remove(vertexID);
				}
			} else{
				throw new RuntimeException("Task with vertex id: " + vertexID + " doesn't have any " +
				" task message handler initialized.");
			}
		}
	}

	/**
	 * Send a message to all parallel instance of task with {@link JobVertexID}
	 *
	 * Called from {@link TaskManager#handleMessage()}
	 *
	 * @param vertexID {@link JobVertexID} of the parallel tasks to send message to
	 * @param message Message to be sent
	 */
	void sendToTask(JobVertexID vertexID, TaskMessage message){
		synchronized (this){
			if(handlers.containsKey(vertexID)){
				handlers.get(vertexID).receive(message);
			}
			// otherwise just throw this message away. We're not meant to do anything to do with this
			// message since this task is not registered with us.
		}
	}

	private void error(){
		if(isShutDown){
			throw new RuntimeException("The Message handler has been shut down by the task manager.");
		}
	}
}
