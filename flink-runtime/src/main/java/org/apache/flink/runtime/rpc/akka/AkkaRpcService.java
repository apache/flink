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
import org.apache.flink.runtime.rpc.MainThreadExecutor;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Future;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.HashSet;

/**
 * Akka based {@link RpcService} implementation. The rpc service starts an Akka actor to receive
 * rpcs from a {@link RpcGateway}.
 */
public class AkkaRpcService implements RpcService {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

	private final ActorSystem actorSystem;
	private final Timeout timeout;
	private final Collection<ActorRef> actors = new HashSet<>(4);

	public AkkaRpcService(final ActorSystem actorSystem, final Timeout timeout) {
		this.actorSystem = Preconditions.checkNotNull(actorSystem, "actor system");
		this.timeout = Preconditions.checkNotNull(timeout, "timeout");
	}

	@Override
	public <C extends RpcGateway> Future<C> connect(final String address, final Class<C> clazz) {
		LOG.info("Try to connect to remote rpc server with address {}. Returning a {} gateway.", address, clazz.getName());

		final ActorSelection actorSel = actorSystem.actorSelection(address);

		final AskableActorSelection asker = new AskableActorSelection(actorSel);

		final Future<Object> identify = asker.ask(new Identify(42), timeout);

		return identify.map(new Mapper<Object, C>(){
			@Override
			public C apply(Object obj) {
				ActorRef actorRef = ((ActorIdentity) obj).getRef();

				InvocationHandler akkaInvocationHandler = new AkkaInvocationHandler(actorRef, timeout);

				@SuppressWarnings("unchecked")
				C proxy = (C) Proxy.newProxyInstance(
					ClassLoader.getSystemClassLoader(),
					new Class<?>[] {clazz},
					akkaInvocationHandler);

				return proxy;
			}
		}, actorSystem.dispatcher());
	}

	@Override
	public <C extends RpcGateway, S extends RpcEndpoint<C>> C startServer(S rpcEndpoint) {
		Preconditions.checkNotNull(rpcEndpoint, "rpc endpoint");

		LOG.info("Start Akka rpc actor to handle rpcs for {}.", rpcEndpoint.getClass().getName());

		Props akkaRpcActorProps = Props.create(AkkaRpcActor.class, rpcEndpoint);

		ActorRef actorRef = actorSystem.actorOf(akkaRpcActorProps);
		actors.add(actorRef);

		InvocationHandler akkaInvocationHandler = new AkkaInvocationHandler(actorRef, timeout);

		@SuppressWarnings("unchecked")
		C self = (C) Proxy.newProxyInstance(
			ClassLoader.getSystemClassLoader(),
			new Class<?>[]{rpcEndpoint.getSelfGatewayType(), MainThreadExecutor.class, AkkaGateway.class},
			akkaInvocationHandler);

		return self;
	}

	@Override
	public <C extends RpcGateway> void stopServer(C selfGateway) {
		if (selfGateway instanceof AkkaGateway) {
			AkkaGateway akkaClient = (AkkaGateway) selfGateway;

			if (actors.contains(akkaClient.getRpcServer())) {
				ActorRef selfActorRef = akkaClient.getRpcServer();

				LOG.info("Stop Akka rpc actor {}.", selfActorRef.path());

				selfActorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
			}
		}
	}

	@Override
	public void stopService() {
		LOG.info("Stop Akka rpc service.");
		actorSystem.shutdown();
		actorSystem.awaitTermination();
	}

	@Override
	public <C extends RpcGateway> String getAddress(C selfGateway) {
		if (selfGateway instanceof AkkaGateway) {
			ActorRef actorRef = ((AkkaGateway) selfGateway).getRpcServer();
			return AkkaUtils.getAkkaURL(actorSystem, actorRef);
		} else {
			String className = AkkaGateway.class.getName();
			throw new RuntimeException("Cannot get address for non " + className + '.');
		}
	}
}
