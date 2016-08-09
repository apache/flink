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

package org.apache.flink.runtime.rpc.akka.resourcemanager;

import akka.actor.ActorRef;
import akka.pattern.AskableActorRef;
import akka.util.Timeout;
import org.apache.flink.runtime.rpc.akka.BaseAkkaGateway;
import org.apache.flink.runtime.rpc.resourcemanager.JobMasterRegistration;
import org.apache.flink.runtime.rpc.resourcemanager.RegistrationResponse;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.resourcemanager.SlotAssignment;
import org.apache.flink.runtime.rpc.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rpc.akka.messages.RegisterJobMaster;
import org.apache.flink.runtime.rpc.akka.messages.RequestSlot;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag$;

public class ResourceManagerAkkaGateway extends BaseAkkaGateway implements ResourceManagerGateway {
	private final AskableActorRef actorRef;
	private final Timeout timeout;

	public ResourceManagerAkkaGateway(ActorRef actorRef, Timeout timeout) {
		this.actorRef = new AskableActorRef(actorRef);
		this.timeout = timeout;
	}

	@Override
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration, FiniteDuration timeout) {
		return actorRef.ask(new RegisterJobMaster(jobMasterRegistration), new Timeout(timeout))
			.mapTo(ClassTag$.MODULE$.<RegistrationResponse>apply(RegistrationResponse.class));
	}

	@Override
	public Future<RegistrationResponse> registerJobMaster(JobMasterRegistration jobMasterRegistration) {
		return actorRef.ask(new RegisterJobMaster(jobMasterRegistration), timeout)
			.mapTo(ClassTag$.MODULE$.<RegistrationResponse>apply(RegistrationResponse.class));
	}

	@Override
	public Future<SlotAssignment> requestSlot(SlotRequest slotRequest) {
		return actorRef.ask(new RequestSlot(slotRequest), timeout)
			.mapTo(ClassTag$.MODULE$.<SlotAssignment>apply(SlotAssignment.class));
	}

	@Override
	public ActorRef getActorRef() {
		return actorRef.actorRef();
	}
}
