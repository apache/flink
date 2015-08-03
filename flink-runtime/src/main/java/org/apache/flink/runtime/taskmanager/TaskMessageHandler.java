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
import org.apache.flink.runtime.messages.TaskMessages;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Message handler for all parallel instances of a {@link Task}
 *
 * 1. {@link TaskManager} has access to this handler and uses it to send messages to Tasks.
 * 2. {@link org.apache.flink.api.common.functions.RuntimeContext} has access to this handler
 *    and uses it to broadcast to its parallel instances and receive from its parallel instances.
 */
public class TaskMessageHandler{
	// queue related fields
	private TaskMessage[] data;
	private HashMap<Integer, Integer> heads;
	private int tail;
	private int maxSize;

	// send-receive related fields
	private JobVertexID vertexID;
	private ActorGateway gateway;
	private boolean isShutDown;

	/**-------------------------------------------------------------------------------------------------
	 * MessageHandler methods
	 * {@link #TaskMessageHandler(int, JobVertexID, ActorGateway)}} via {@link MessageHandler#registerTask(JobVertexID, int)}
	 * {@link #receive(TaskMessage)} via {@link MessageHandler#sendToTask(JobVertexID, TaskMessage)}
	 * {@link #subscribe(int)} via {@link MessageHandler#registerTask(JobVertexID, int)}
	 * {@link #unSubscribe(int)} via {@link MessageHandler#unregisterTask(JobVertexID, int)}
	 * {@link #shutDown()} via {@link MessageHandler#shutDown} and {@link MessageHandler#unregisterTask(JobVertexID, int)}
	 ----------------------------------------------------------------------------------------------------*/
	/**
	 * Create a new instance of TaskMessageHandler to handle message for a particular task. This will be created
	 * by the {@link TaskManager} and passed on to {@link org.apache.flink.api.common.functions.RuntimeContext}.
	 * All parallel instances of a task running under the parent task manager will send and receive their
	 * messages via this TaskMessageHandler.
	 *
	 * @param maxSize Maximum number of messages allowed to be store
	 * @param vertexID Vertex ID in the {@link org.apache.flink.runtime.jobgraph.JobGraph} for this task
	 * @param gateway Gateway to the {@link TaskManager}
	 *
	 */
	public TaskMessageHandler(int maxSize, JobVertexID vertexID, ActorGateway gateway){
		this.data = new TaskMessage[maxSize];
		this.heads = new HashMap<Integer, Integer>();
		this.tail = -1;
		this.vertexID = vertexID;
		this.gateway = gateway;
		this.isShutDown = false;
		this.maxSize = maxSize;
	}

	/**
	 * Receive a message from the {@link MessageHandler}. This message is meant for all parallel running
	 * instances of task with vertex ID {@link this.vertexID} and must be delivered to everyone.
	 * Called by {@link MessageHandler}
	 *
	 * @param message Message to be provided to tasks with vertexID = {@link this.vertexID}
	 */
	void receive(TaskMessage message){
		synchronized (this){
			error();
			if(tail == -1){
				data[0] = message;
				tail = 1 % maxSize;
				return;
			}
			for(int head: heads.values()){
				if(head == tail){
					throw new RuntimeException("The message queue is full. Try improving your program to " +
					"consume as fast as it produces. Or set a bigger queue size in your conf file");
				}
			}
			data[tail] = message;
			tail = (tail + 1) % maxSize;
		}
	}

	/**
	 * Subscribe the task with subTaskIndex to receive messages
	 */
	void subscribe(int subTaskIndex){
		synchronized (this) {
			if (heads.containsKey(subTaskIndex)) {
				// maybe we should throw an exception. I'll come back to this.
			} else {
				// we'll give this parallel instance any messages we have in store.
				heads.put(subTaskIndex, -1);
			}
		}
	}

	/**
	 * Un-subscribe the task with subTaskIndex from receiving anything
	 */
	boolean unSubscribe(int subTaskIndex){
		synchronized (this){
			if(heads.containsKey(subTaskIndex)){
				// like I said, maybe we throw an exception
			} else{
				heads.remove(subTaskIndex);
			}
			// if all tasks have unSubscribed, we can just shut ourselves down.
			if(heads.size() == 0){
				return true;
			} else{
				return false;
			}
		}
	}

	/**
	 * Shut down this task message handler. Clear out all resources.
	 */
	void shutDown(){
		synchronized (this){
			// clear out our heap memory
			heads.clear();
			data = null;
			isShutDown = true;
			tail = -1;
		}
	}

	/** -----------------------------------------------------------------------------------------------------
	 * User methods:
	 * {@link #send(TaskMessage)} via {@link org.apache.flink.api.common.functions.RuntimeContext#broadcast}
	 * {@link #fetch(int)} via {@link org.apache.flink.api.common.functions.RuntimeContext#receive}
	 --------------------------------------------------------------------------------------------------------*/

	/**
	 * Broadcast a message to every {@link TaskManager} This will be received by every parallel running
	 * instance of this task.
	 *
	 * @param message Message to be broadcasted
	 */
	public void send(TaskMessage message){
		synchronized (this){
			error();
			gateway.tell(new TaskMessages.SelfBroadcast(new TaskMessages.RuntimeMessage(vertexID, message)));
		}
	}

	/**
	 * Make available every message meant for the task with vertex ID {@link this.vertexID} and index subTaskIndex.
	 * This is on a only once basis, and once provided to a parallel instance, the messages will never be
	 * provided to the same parallel instance ever again.
	 *
	 * @param subTaskIndex index of the task accessing the messages
	 */
	public List<TaskMessage> fetch(int subTaskIndex){
		synchronized (this){
			if(heads.containsKey(subTaskIndex)){
				if(tail == -1){
					return new LinkedList<TaskMessage>();
				}
				int head = (heads.get(subTaskIndex) + 1) % maxSize;
				LinkedList<TaskMessage> result = new LinkedList<TaskMessage>();
				if(head <= tail){
					head = head + maxSize;
				}
				while(head < tail + maxSize){
					result.add(data[head % maxSize]);
					head++;
				}
				heads.put(subTaskIndex, head - maxSize - 1);
				return result;
			} else{
				throw new RuntimeException("The sub task index: " + subTaskIndex + " has been unregistered from " +
				"receiving any messages by the task manager.");
			}
		}
	}

	private void error(){
		if(isShutDown){
			throw new RuntimeException("Task Message handler associated with the vertex id: " + vertexID.toString() +
			" has been shut down.");
		}
	}
}
