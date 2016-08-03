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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.dispatch.Mapper;
import akka.pattern.AskableActorSelection;
import akka.util.Timeout;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.jobmaster.JobMaster;
import org.apache.flink.runtime.rpc.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.jobmaster.JobMasterAkkaActor;
import org.apache.flink.runtime.rpc.akka.jobmaster.JobMasterAkkaGateway;
import org.apache.flink.runtime.rpc.akka.resourcemanager.ResourceManagerAkkaActor;
import org.apache.flink.runtime.rpc.akka.resourcemanager.ResourceManagerAkkaGateway;
import org.apache.flink.runtime.rpc.akka.taskexecutor.TaskExecutorAkkaActor;
import org.apache.flink.runtime.rpc.akka.taskexecutor.TaskExecutorAkkaGateway;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.rpc.taskexecutor.TaskExecutor;
import scala.concurrent.Future;

import java.util.HashSet;
import java.util.Set;

public class AkkaRpcService implements RpcService {
	private final ActorSystem actorSystem;
	private final Timeout timeout;
	private final Set<ActorRef> actors = new HashSet<>();

	public AkkaRpcService(ActorSystem actorSystem, Timeout timeout) {
		this.actorSystem = actorSystem;
		this.timeout = timeout;
	}

	@Override
	public <C extends RpcGateway> Future<C> connect(String address, final Class<C> clazz) {
		ActorSelection actorSel = actorSystem.actorSelection(address);

		AskableActorSelection asker = new AskableActorSelection(actorSel);

		Future<Object> identify = asker.ask(new Identify(42), timeout);

		return identify.map(new Mapper<Object, C>(){
			public C apply(Object obj) {
				ActorRef actorRef = ((ActorIdentity) obj).getRef();

				if (clazz == TaskExecutorGateway.class) {
					return (C) new TaskExecutorAkkaGateway(actorRef, timeout);
				} else if (clazz == ResourceManagerGateway.class) {
					return (C) new ResourceManagerAkkaGateway(actorRef, timeout);
				} else if (clazz == JobMasterGateway.class) {
					return (C) new JobMasterAkkaGateway(actorRef, timeout);
				} else {
					throw new RuntimeException("Could not find remote endpoint " + clazz);
				}
			}
		}, actorSystem.dispatcher());
	}

	@Override
	public <S extends RpcEndpoint, C extends RpcGateway> C startServer(S rpcEndpoint) {
		ActorRef ref;
		C self;
		if (rpcEndpoint instanceof TaskExecutor) {
			ref = actorSystem.actorOf(
				Props.create(TaskExecutorAkkaActor.class, rpcEndpoint)
			);

			self = (C) new TaskExecutorAkkaGateway(ref, timeout);
		} else if (rpcEndpoint instanceof ResourceManager) {
			ref = actorSystem.actorOf(
				Props.create(ResourceManagerAkkaActor.class, rpcEndpoint)
			);

			self = (C) new ResourceManagerAkkaGateway(ref, timeout);
		} else if (rpcEndpoint instanceof JobMaster) {
			ref = actorSystem.actorOf(
				Props.create(JobMasterAkkaActor.class, rpcEndpoint)
			);

			self = (C) new JobMasterAkkaGateway(ref, timeout);
		} else {
			throw new RuntimeException("Could not start RPC server for class " + rpcEndpoint.getClass());
		}

		actors.add(ref);

		return self;
	}

	@Override
	public <C extends RpcGateway> void stopServer(C selfGateway) {
		if (selfGateway instanceof AkkaGateway) {
			AkkaGateway akkaClient = (AkkaGateway) selfGateway;

			if (actors.contains(akkaClient.getActorRef())) {
				akkaClient.getActorRef().tell(PoisonPill.getInstance(), ActorRef.noSender());
			} else {
				// don't stop this actor since it was not started by this RPC service
			}
		}
	}

	@Override
	public void stopService() {
		actorSystem.shutdown();
		actorSystem.awaitTermination();
	}

	@Override
	public <C extends RpcGateway> String getAddress(C selfGateway) {
		if (selfGateway instanceof AkkaGateway) {
			return AkkaUtils.getAkkaURL(actorSystem, ((AkkaGateway) selfGateway).getActorRef());
		} else {
			throw new RuntimeException("Cannot get address for non " + AkkaGateway.class.getName() + ".");
		}
	}
}
