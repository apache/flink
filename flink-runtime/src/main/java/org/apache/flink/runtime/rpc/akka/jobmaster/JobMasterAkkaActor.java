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

package org.apache.flink.runtime.rpc.akka.jobmaster;

import akka.actor.ActorRef;
import akka.actor.Status;
import org.apache.flink.runtime.rpc.akka.RunnableAkkaActor;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.akka.messages.HandleRegistrationResponse;
import org.apache.flink.runtime.rpc.akka.messages.TriggerResourceManagerRegistration;
import org.apache.flink.runtime.rpc.akka.messages.UpdateTaskExecutionState;

public class JobMasterAkkaActor extends RunnableAkkaActor {
	private final JobMaster jobMaster;

	public JobMasterAkkaActor(JobMaster jobMaster) {
		this.jobMaster = jobMaster;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof UpdateTaskExecutionState) {

			final ActorRef sender = getSender();

			UpdateTaskExecutionState updateTaskExecutionState = (UpdateTaskExecutionState) message;

			try {
				Acknowledge result = jobMaster.updateTaskExecutionState(updateTaskExecutionState.getTaskExecutionState());
				sender.tell(new Status.Success(result), getSelf());
			} catch (Exception e) {
				sender.tell(new Status.Failure(e), getSelf());
			}
		} else if (message instanceof TriggerResourceManagerRegistration) {
			TriggerResourceManagerRegistration triggerResourceManagerRegistration = (TriggerResourceManagerRegistration) message;

			jobMaster.triggerResourceManagerRegistration(triggerResourceManagerRegistration.getAddress());
		} else if (message instanceof HandleRegistrationResponse) {
			HandleRegistrationResponse registrationResponse = (HandleRegistrationResponse) message;

			jobMaster.handleRegistrationResponse(registrationResponse.getRegistrationResponse(), registrationResponse.getResourceManagerGateway());
		} else {
			super.onReceive(message);
		}
	}
}
