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
import akka.pattern.AskableActorRef;
import akka.util.Timeout;
import org.apache.flink.runtime.rpc.akka.RunnableAkkaGateway;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.resourcemanager.RegistrationResponse;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.akka.messages.HandleRegistrationResponse;
import org.apache.flink.runtime.rpc.akka.messages.TriggerResourceManagerRegistration;
import org.apache.flink.runtime.rpc.akka.messages.UpdateTaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import scala.concurrent.Future;
import scala.reflect.ClassTag$;

public class JobMasterAkkaGateway extends RunnableAkkaGateway implements JobMasterGateway {
	private final AskableActorRef actorRef;
	private final Timeout timeout;

	public JobMasterAkkaGateway(ActorRef actorRef, Timeout timeout) {
		this.actorRef = new AskableActorRef(actorRef);
		this.timeout = timeout;
	}

	@Override
	public Future<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		return actorRef.ask(new UpdateTaskExecutionState(taskExecutionState), timeout)
			.mapTo(ClassTag$.MODULE$.<Acknowledge>apply(Acknowledge.class));
	}

	@Override
	public void triggerResourceManagerRegistration(String address) {
		actorRef.actorRef().tell(new TriggerResourceManagerRegistration(address), ActorRef.noSender());
	}

	@Override
	public void handleRegistrationResponse(RegistrationResponse response, ResourceManagerGateway resourceManager) {
		actorRef.actorRef().tell(new HandleRegistrationResponse(response, resourceManager), ActorRef.noSender());
	}

	@Override
	public ActorRef getActorRef() {
		return actorRef.actorRef();
	}
}
