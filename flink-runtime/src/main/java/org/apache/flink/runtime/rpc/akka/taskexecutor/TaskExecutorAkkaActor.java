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

package org.apache.flink.runtime.rpc.akka.taskexecutor;

import akka.actor.ActorRef;
import akka.actor.Status;
import akka.dispatch.OnComplete;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.akka.BaseAkkaActor;
import org.apache.flink.runtime.rpc.akka.messages.CancelTask;
import org.apache.flink.runtime.rpc.akka.messages.ExecuteTask;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;

public class TaskExecutorAkkaActor extends BaseAkkaActor {
	private final TaskExecutorGateway taskExecutor;

	public TaskExecutorAkkaActor(TaskExecutorGateway taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		final ActorRef sender = getSender();

		if (message instanceof ExecuteTask) {
			ExecuteTask executeTask = (ExecuteTask) message;

			taskExecutor.executeTask(executeTask.getTaskDeploymentDescriptor()).onComplete(
				new OnComplete<Acknowledge>() {
					@Override
					public void onComplete(Throwable failure, Acknowledge success) throws Throwable {
						if (failure != null) {
							sender.tell(new Status.Failure(failure), getSelf());
						} else {
							sender.tell(new Status.Success(Acknowledge.get()), getSelf());
						}
					}
				},
				getContext().dispatcher()
			);
		} else if (message instanceof CancelTask) {
			CancelTask cancelTask = (CancelTask) message;

			taskExecutor.cancelTask(cancelTask.getExecutionAttemptID()).onComplete(
				new OnComplete<Acknowledge>() {
					@Override
					public void onComplete(Throwable failure, Acknowledge success) throws Throwable {
						if (failure != null) {
							sender.tell(new Status.Failure(failure), getSelf());
						} else {
							sender.tell(new Status.Success(Acknowledge.get()), getSelf());
						}
					}
				},
				getContext().dispatcher()
			);
		} else {
			super.onReceive(message);
		}
	}
}
