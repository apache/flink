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
package org.apache.flink.runtime.rpc.jobmaster;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import scala.concurrent.Future;

/**
 * {@link TaskControllerGateway} is responsible to act to changes of task execution and result partition
 */
public interface TaskControllerGateway extends RpcGateway {
	/**
	 * handle task state update message from task manager
	 * @param state New task execution state for a given task
	 * @return acknowledge of the task execution state update
	 */
	Future<Acknowledge> updateTaskExecutionState(TaskExecutionState state);

	/**
	 * trigger to schedule consumer task of the given result partition, if the task is running,
	 * trigger update RPC to the task.
	 * @param resultPartitionID the ID of result partition to trigger
	 */
	void scheduleOrUpdateConsumer(ResultPartitionID resultPartitionID);
}
