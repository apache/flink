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
import akka.dispatch.Futures;
import akka.dispatch.Mapper;

import akka.pattern.Patterns;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive
 * RPC invocations from a {@link RpcGateway}.
 */
@ThreadSafe
public class AkkaRpcService implements RpcService {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

	static final String MAXIMUM_FRAME_SIZE_PATH = "akka.remote.netty.tcp.maximum-frame-size";

	private final Object lock = new Object();

	private final ActorSystem actorSystem;
	private final Time timeout;
	private final Set<ActorRef> actors = new HashSet<>(4);
	private final long maximumFramesize;

	private volatile boolean stopped;

	public AkkaRpcService(final ActorSystem actorSystem, final Time timeout) {
		this.actorSystem = checkNotNull(actorSystem, "actor system");
		this.timeout = checkNotNull(timeout, "timeout");

		if (actorSystem.settings().config().hasPath(MAXIMUM_FRAME_SIZE_PATH)) {
			maximumFramesize = actorSystem.settings().config().getBytes(MAXIMUM_FRAME_SIZE_PATH);
		} else {
			// only local communication
			maximumFramesize = Long.MAX_VALUE;
		}
	}

	// this method does not mutate state and is thus thread-safe
	@Override
	public <C extends RpcGateway> Future<C> connect(final String address, final Class<C> clazz) {
		checkState(!stopped, "RpcService is stopped");

		LOG.debug("Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
				address, clazz.getName());

		final ActorSelection actorSel = actorSystem.actorSelection(address);

		final scala.concurrent.Future<Object> identify = Patterns.ask(actorSel, new Identify(42), timeout.toMilliseconds());
		final scala.concurrent.Future<C> resultFuture = identify.map(new Mapper<Object, C>(){
			@Override
			public C checkedApply(Object obj) throws Exception {

				ActorIdentity actorIdentity = (ActorIdentity) obj;

				if (actorIdentity.getRef() == null) {
					throw new RpcConnectionException("Could not connect to rpc endpoint under address " + address + '.');
				} else {
					ActorRef actorRef = actorIdentity.getRef();

					final String address = AkkaUtils.getAkkaURL(actorSystem, actorRef);

					InvocationHandler akkaInvocationHandler = new AkkaInvocationHandler(address, actorRef, timeout, maximumFramesize);

					// Rather than using the System ClassLoader directly, we derive the ClassLoader
					// from this class . That works better in cases where Flink runs embedded and all Flink
					// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
					ClassLoader classLoader = AkkaRpcService.this.getClass().getClassLoader();

					@SuppressWarnings("unchecked")
					C proxy = (C) Proxy.newProxyInstance(
						classLoader,
						new Class<?>[]{clazz},
						akkaInvocationHandler);

					return proxy;
				}
			}
		}, actorSystem.dispatcher());

		return new FlinkFuture<>(resultFuture);
	}

	@Override
	public <C extends RpcGateway, S extends RpcEndpoint<C>> C startServer(S rpcEndpoint) {
		checkNotNull(rpcEndpoint, "rpc endpoint");

		Props akkaRpcActorProps = Props.create(AkkaRpcActor.class, rpcEndpoint);
		ActorRef actorRef;

		synchronized (lock) {
			checkState(!stopped, "RpcService is stopped");
			actorRef = actorSystem.actorOf(akkaRpcActorProps);
			actors.add(actorRef);
		}

		LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());

		final String address = AkkaUtils.getAkkaURL(actorSystem, actorRef);

		InvocationHandler akkaInvocationHandler = new AkkaInvocationHandler(address, actorRef, timeout, maximumFramesize);

		// Rather than using the System ClassLoader directly, we derive the ClassLoader
		// from this class . That works better in cases where Flink runs embedded and all Flink
		// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
		ClassLoader classLoader = getClass().getClassLoader();

		@SuppressWarnings("unchecked")
		C self = (C) Proxy.newProxyInstance(
			classLoader,
			new Class<?>[]{
				rpcEndpoint.getSelfGatewayType(),
				MainThreadExecutable.class,
				StartStoppable.class,
				AkkaGateway.class},
			akkaInvocationHandler);

		return self;
	}

	@Override
	public void stopServer(RpcGateway selfGateway) {
		if (selfGateway instanceof AkkaGateway) {
			AkkaGateway akkaClient = (AkkaGateway) selfGateway;

			boolean fromThisService;
			synchronized (lock) {
				if (stopped) {
					return;
				} else {
					fromThisService = actors.remove(akkaClient.getRpcEndpoint());
				}
			}

			if (fromThisService) {
				ActorRef selfActorRef = akkaClient.getRpcEndpoint();
				LOG.info("Stopping RPC endpoint {}.", selfActorRef.path());
				selfActorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
			} else {
				LOG.debug("RPC endpoint {} already stopped or from different RPC service");
			}
		}
	}

	@Override
	public void stopService() {
		LOG.info("Stopping Akka RPC service.");

		synchronized (lock) {
			if (stopped) {
				return;
			}

			stopped = true;
			actorSystem.shutdown();
			actors.clear();
		}

		actorSystem.awaitTermination();
	}

	@Override
	public Executor getExecutor() {
		return actorSystem.dispatcher();
	}

	@Override
	public void scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
		checkNotNull(runnable, "runnable");
		checkNotNull(unit, "unit");
		checkArgument(delay >= 0, "delay must be zero or larger");

		actorSystem.scheduler().scheduleOnce(new FiniteDuration(delay, unit), runnable, actorSystem.dispatcher());
	}

	@Override
	public void execute(Runnable runnable) {
		actorSystem.dispatcher().execute(runnable);
	}

	@Override
	public <T> Future<T> execute(Callable<T> callable) {
		scala.concurrent.Future<T> scalaFuture = Futures.future(callable, actorSystem.dispatcher());

		return new FlinkFuture<>(scalaFuture);
	}
}
