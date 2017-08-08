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
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.actor.Identify;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.messages.Shutdown;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
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

	private final String address;
	private final int port;

	private final ScheduledExecutor internalScheduledExecutor;

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

		Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);

		if (actorSystemAddress.host().isDefined()) {
			address = actorSystemAddress.host().get();
		} else {
			address = "";
		}

		if (actorSystemAddress.port().isDefined()) {
			port = (Integer) actorSystemAddress.port().get();
		} else {
			port = -1;
		}

		internalScheduledExecutor = new InternalScheduledExecutorImpl(actorSystem);
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public int getPort() {
		return port;
	}

	// this method does not mutate state and is thus thread-safe
	@Override
	public <C extends RpcGateway> CompletableFuture<C> connect(final String address, final Class<C> clazz) {
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
					final String hostname;
					Option<String> host = actorRef.path().address().host();
					if (host.isEmpty()) {
						hostname = "localhost";
					} else {
						hostname = host.get();
					}

					InvocationHandler akkaInvocationHandler = new AkkaInvocationHandler(
						address,
						hostname,
						actorRef,
						timeout,
						maximumFramesize,
						null);

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

		return FutureUtils.toJava(resultFuture);
	}

	@Override
	public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
		checkNotNull(rpcEndpoint, "rpc endpoint");

		CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
		Props akkaRpcActorProps = Props.create(AkkaRpcActor.class, rpcEndpoint, terminationFuture);
		ActorRef actorRef;

		synchronized (lock) {
			checkState(!stopped, "RpcService is stopped");
			actorRef = actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId());
			actors.add(actorRef);
		}

		LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());

		final String address = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}

		InvocationHandler akkaInvocationHandler = new AkkaInvocationHandler(
			address,
			hostname,
			actorRef,
			timeout,
			maximumFramesize,
			terminationFuture);

		// Rather than using the System ClassLoader directly, we derive the ClassLoader
		// from this class . That works better in cases where Flink runs embedded and all Flink
		// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
		ClassLoader classLoader = getClass().getClassLoader();

		Set<Class<? extends RpcGateway>> implementedRpcGateways = RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass());

		implementedRpcGateways.add(RpcServer.class);
		implementedRpcGateways.add(AkkaGateway.class);

		@SuppressWarnings("unchecked")
		RpcServer server = (RpcServer) Proxy.newProxyInstance(
			classLoader,
			implementedRpcGateways.toArray(new Class<?>[implementedRpcGateways.size()]),
			akkaInvocationHandler);

		return server;
	}

	@Override
	public void stopServer(RpcServer selfGateway) {
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
				LOG.info("Trigger shut down of RPC endpoint {}.", selfActorRef.path());
				selfActorRef.tell(Shutdown.getInstance(), ActorRef.noSender());
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
	public CompletableFuture<Void> getTerminationFuture() {
		return CompletableFuture.runAsync(
			actorSystem::awaitTermination,
			getExecutor());
	}

	@Override
	public Executor getExecutor() {
		return actorSystem.dispatcher();
	}

	public ScheduledExecutor getScheduledExecutor() {
		return internalScheduledExecutor;
	}

	@Override
	public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
		checkNotNull(runnable, "runnable");
		checkNotNull(unit, "unit");
		checkArgument(delay >= 0L, "delay must be zero or larger");

		return internalScheduledExecutor.schedule(runnable, delay, unit);
	}

	@Override
	public void execute(Runnable runnable) {
		actorSystem.dispatcher().execute(runnable);
	}

	@Override
	public <T> CompletableFuture<T> execute(Callable<T> callable) {
		Future<T> scalaFuture = Futures.future(callable, actorSystem.dispatcher());

		return FutureUtils.toJava(scalaFuture);
	}

	/**
	 * Helper class to expose the internal scheduling logic via a {@link ScheduledExecutor}.
	 */
	private static final class InternalScheduledExecutorImpl implements ScheduledExecutor {

		private final ActorSystem actorSystem;

		private InternalScheduledExecutorImpl(ActorSystem actorSystem) {
			this.actorSystem = Preconditions.checkNotNull(actorSystem, "rpcService");
		}

		@Override
		@Nonnull
		public ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
			ScheduledFutureTask<Void> scheduledFutureTask = new ScheduledFutureTask<>(command, unit.toNanos(delay), 0L);

			Cancellable cancellable = internalSchedule(scheduledFutureTask, delay, unit);

			scheduledFutureTask.setCancellable(cancellable);

			return scheduledFutureTask;
		}

		@Override
		@Nonnull
		public <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
			ScheduledFutureTask<V> scheduledFutureTask = new ScheduledFutureTask<>(callable, unit.toNanos(delay), 0L);

			Cancellable cancellable = internalSchedule(scheduledFutureTask, delay, unit);

			scheduledFutureTask.setCancellable(cancellable);

			return scheduledFutureTask;
		}

		@Override
		@Nonnull
		public ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period, @Nonnull TimeUnit unit) {
			ScheduledFutureTask<Void> scheduledFutureTask = new ScheduledFutureTask<>(
				command,
				triggerTime(unit.toNanos(initialDelay)),
				unit.toNanos(period));

			Cancellable cancellable = actorSystem.scheduler().schedule(
				new FiniteDuration(initialDelay, unit),
				new FiniteDuration(period, unit),
				scheduledFutureTask,
				actorSystem.dispatcher());

			scheduledFutureTask.setCancellable(cancellable);

			return scheduledFutureTask;
		}

		@Override
		@Nonnull
		public ScheduledFuture<?> scheduleWithFixedDelay(@Nonnull Runnable command, long initialDelay, long delay, @Nonnull TimeUnit unit) {
			ScheduledFutureTask<Void> scheduledFutureTask = new ScheduledFutureTask<>(
				command,
				triggerTime(unit.toNanos(initialDelay)),
				unit.toNanos(-delay));

			Cancellable cancellable = internalSchedule(scheduledFutureTask, initialDelay, unit);

			scheduledFutureTask.setCancellable(cancellable);

			return scheduledFutureTask;
		}

		@Override
		public void execute(@Nonnull Runnable command) {
			actorSystem.dispatcher().execute(command);
		}

		private Cancellable internalSchedule(Runnable runnable, long delay, TimeUnit unit) {
			return actorSystem.scheduler().scheduleOnce(
				new FiniteDuration(delay, unit),
				runnable,
				actorSystem.dispatcher());
		}

		private long now() {
			return System.nanoTime();
		}

		private long triggerTime(long delay) {
			return now() + delay;
		}

		private final class ScheduledFutureTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {

			private long time;

			private final long period;

			private volatile Cancellable cancellable;

			ScheduledFutureTask(Callable<V> callable, long time, long period) {
				super(callable);
				this.time = time;
				this.period = period;
			}

			ScheduledFutureTask(Runnable runnable, long time, long period) {
				super(runnable, null);
				this.time = time;
				this.period = period;
			}

			public void setCancellable(Cancellable newCancellable) {
				this.cancellable = newCancellable;
			}

			@Override
			public void run() {
				if (!isPeriodic()) {
					super.run();
				} else if (runAndReset()){
					if (period > 0L) {
						time += period;
					} else {
						cancellable = internalSchedule(this, -period, TimeUnit.NANOSECONDS);

						// check whether we have been cancelled concurrently
						if (isCancelled()) {
							cancellable.cancel();
						} else {
							time = triggerTime(-period);
						}
					}
				}
			}

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				boolean result = super.cancel(mayInterruptIfRunning);

				return result && cancellable.cancel();
			}

			@Override
			public long getDelay(@Nonnull  TimeUnit unit) {
				return unit.convert(time - now(), TimeUnit.NANOSECONDS);
			}

			@Override
			public int compareTo(@Nonnull Delayed o) {
				if (o == this) {
					return 0;
				}

				long diff = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
				return (diff < 0L) ? -1 : (diff > 0L) ? 1 : 0;
			}

			@Override
			public boolean isPeriodic() {
				return period != 0L;
			}
		}
	}
}
