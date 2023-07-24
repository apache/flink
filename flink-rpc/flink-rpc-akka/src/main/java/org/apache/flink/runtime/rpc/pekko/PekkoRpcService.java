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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.pekko.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.exceptions.RpcRuntimeException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSelection;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.DeadLetter;
import org.apache.pekko.actor.Props;
import org.apache.pekko.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.reflect.ClassTag$;

import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.guardCompletionWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.withContextClassLoader;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Pekko based {@link RpcService} implementation. The RPC service starts an actor to receive RPC
 * invocations from a {@link RpcGateway}.
 */
@ThreadSafe
public class PekkoRpcService implements RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(PekkoRpcService.class);

    static final int VERSION = 2;

    private final Object lock = new Object();

    private final ActorSystem actorSystem;
    private final PekkoRpcServiceConfiguration configuration;

    private final ClassLoader flinkClassLoader;

    @GuardedBy("lock")
    private final Map<ActorRef, RpcEndpoint> actors = CollectionUtil.newHashMapWithExpectedSize(4);

    private final String address;
    private final int port;

    private final boolean captureAskCallstacks;

    private final ScheduledExecutor internalScheduledExecutor;

    private final CompletableFuture<Void> terminationFuture;

    private final Supervisor supervisor;

    private volatile boolean stopped;

    @VisibleForTesting
    public PekkoRpcService(
            final ActorSystem actorSystem, final PekkoRpcServiceConfiguration configuration) {
        this(actorSystem, configuration, PekkoRpcService.class.getClassLoader());
    }

    PekkoRpcService(
            final ActorSystem actorSystem,
            final PekkoRpcServiceConfiguration configuration,
            final ClassLoader flinkClassLoader) {
        this.actorSystem = checkNotNull(actorSystem, "actor system");
        this.configuration = checkNotNull(configuration, "pekko rpc service configuration");
        this.flinkClassLoader = checkNotNull(flinkClassLoader, "flinkClassLoader");

        Address actorSystemAddress = PekkoUtils.getAddress(actorSystem);

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

        captureAskCallstacks = configuration.captureAskCallStack();

        // Pekko always sets the threads context class loader to the class loader with which it was
        // loaded (i.e., the plugin class loader)
        // we must ensure that the context class loader is set to the Flink class loader when we
        // call into Flink
        // otherwise we could leak the plugin class loader or poison the context class loader of
        // external threads (because they inherit the current threads context class loader)
        internalScheduledExecutor =
                new ActorSystemScheduledExecutorAdapter(actorSystem, flinkClassLoader);

        terminationFuture = new CompletableFuture<>();

        stopped = false;

        supervisor = startSupervisorActor();
        startDeadLettersActor();
    }

    private void startDeadLettersActor() {
        final ActorRef deadLettersActor =
                actorSystem.actorOf(DeadLettersActor.getProps(), "deadLettersActor");
        actorSystem.eventStream().subscribe(deadLettersActor, DeadLetter.class);
    }

    private Supervisor startSupervisorActor() {
        final ExecutorService terminationFutureExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "RpcService-Supervisor-Termination-Future-Executor"));
        final ActorRef actorRef =
                SupervisorActor.startSupervisorActor(
                        actorSystem,
                        withContextClassLoader(terminationFutureExecutor, flinkClassLoader));

        return Supervisor.create(actorRef, terminationFutureExecutor);
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    protected int getVersion() {
        return VERSION;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType, RpcServer rpcServer) {
        if (selfGatewayType.isInstance(rpcServer)) {
            @SuppressWarnings("unchecked")
            C selfGateway = ((C) rpcServer);

            return selfGateway;
        } else {
            throw new ClassCastException(
                    "RpcEndpoint does not implement the RpcGateway interface of type "
                            + selfGatewayType
                            + '.');
        }
    }

    // this method does not mutate state and is thus thread-safe
    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(
            final String address, final Class<C> clazz) {

        return connectInternal(
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new PekkoInvocationHandler(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }

    // this method does not mutate state and is thus thread-safe
    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return connectInternal(
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new FencedPekkoInvocationHandler<>(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        checkNotNull(rpcEndpoint, "rpc endpoint");

        final SupervisorActor.ActorRegistration actorRegistration = registerRpcActor(rpcEndpoint);
        final ActorRef actorRef = actorRegistration.getActorRef();
        final CompletableFuture<Void> actorTerminationFuture =
                actorRegistration.getTerminationFuture();

        LOG.info(
                "Starting RPC endpoint for {} at {} .",
                rpcEndpoint.getClass().getName(),
                actorRef.path());

        final String address = PekkoUtils.getRpcURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        Set<Class<?>> implementedRpcGateways =
                new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

        implementedRpcGateways.add(RpcServer.class);
        implementedRpcGateways.add(PekkoBasedEndpoint.class);

        final InvocationHandler invocationHandler;

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            // a FencedRpcEndpoint needs a FencedPekkoInvocationHandler
            invocationHandler =
                    new FencedPekkoInvocationHandler<>(
                            address,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            actorTerminationFuture,
                            ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
        } else {
            invocationHandler =
                    new PekkoInvocationHandler(
                            address,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            actorTerminationFuture,
                            captureAskCallstacks,
                            flinkClassLoader);
        }

        // Rather than using the System ClassLoader directly, we derive the ClassLoader
        // from this class . That works better in cases where Flink runs embedded and all Flink
        // code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
        ClassLoader classLoader = getClass().getClassLoader();

        @SuppressWarnings("unchecked")
        RpcServer server =
                (RpcServer)
                        Proxy.newProxyInstance(
                                classLoader,
                                implementedRpcGateways.toArray(
                                        new Class<?>[implementedRpcGateways.size()]),
                                invocationHandler);

        return server;
    }

    private <C extends RpcEndpoint & RpcGateway> SupervisorActor.ActorRegistration registerRpcActor(
            C rpcEndpoint) {
        final Class<? extends AbstractActor> rpcActorType;

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            rpcActorType = FencedPekkoRpcActor.class;
        } else {
            rpcActorType = PekkoRpcActor.class;
        }

        synchronized (lock) {
            checkState(!stopped, "RpcService is stopped");

            final SupervisorActor.StartRpcActorResponse startRpcActorResponse =
                    SupervisorActor.startRpcActor(
                            supervisor.getActor(),
                            actorTerminationFuture ->
                                    Props.create(
                                            rpcActorType,
                                            rpcEndpoint,
                                            actorTerminationFuture,
                                            getVersion(),
                                            configuration.getMaximumFramesize(),
                                            configuration.isForceRpcInvocationSerialization(),
                                            flinkClassLoader),
                            rpcEndpoint.getEndpointId());

            final SupervisorActor.ActorRegistration actorRegistration =
                    startRpcActorResponse.orElseThrow(
                            cause ->
                                    new RpcRuntimeException(
                                            String.format(
                                                    "Could not create the %s for %s.",
                                                    PekkoRpcActor.class.getSimpleName(),
                                                    rpcEndpoint.getEndpointId()),
                                            cause));

            actors.put(actorRegistration.getActorRef(), rpcEndpoint);

            return actorRegistration;
        }
    }

    @Override
    public void stopServer(RpcServer selfGateway) {
        if (selfGateway instanceof PekkoBasedEndpoint) {
            final PekkoBasedEndpoint client = (PekkoBasedEndpoint) selfGateway;
            final RpcEndpoint rpcEndpoint;

            synchronized (lock) {
                if (stopped) {
                    return;
                } else {
                    rpcEndpoint = actors.remove(client.getActorRef());
                }
            }

            if (rpcEndpoint != null) {
                terminateRpcActor(client.getActorRef(), rpcEndpoint);
            } else {
                LOG.debug(
                        "RPC endpoint {} already stopped or from different RPC service",
                        selfGateway.getAddress());
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> rpcActorsTerminationFuture;

        synchronized (lock) {
            if (stopped) {
                return terminationFuture;
            }

            LOG.info("Stopping Pekko RPC service.");

            stopped = true;

            rpcActorsTerminationFuture = terminateRpcActors();
        }

        final CompletableFuture<Void> supervisorTerminationFuture =
                FutureUtils.composeAfterwards(rpcActorsTerminationFuture, supervisor::closeAsync);

        final CompletableFuture<Void> actorSystemTerminationFuture =
                FutureUtils.composeAfterwards(
                        supervisorTerminationFuture,
                        () -> ScalaFutureUtils.toJava(actorSystem.terminate()));

        actorSystemTerminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> {
                    runWithContextClassLoader(
                            () -> FutureUtils.doForward(ignored, throwable, terminationFuture),
                            flinkClassLoader);

                    LOG.info("Stopped Pekko RPC service.");
                });

        return terminationFuture;
    }

    @GuardedBy("lock")
    @Nonnull
    private CompletableFuture<Void> terminateRpcActors() {
        final Collection<CompletableFuture<Void>> rpcActorTerminationFutures =
                new ArrayList<>(actors.size());

        for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
            rpcActorTerminationFutures.add(
                    terminateRpcActor(
                            actorRefRpcEndpointEntry.getKey(),
                            actorRefRpcEndpointEntry.getValue()));
        }
        actors.clear();

        return FutureUtils.waitForAll(rpcActorTerminationFutures);
    }

    private CompletableFuture<Void> terminateRpcActor(
            ActorRef rpcActorRef, RpcEndpoint rpcEndpoint) {
        rpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

        return rpcEndpoint.getTerminationFuture();
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return internalScheduledExecutor;
    }

    // ---------------------------------------------------------------------------------------
    // Private helper methods
    // ---------------------------------------------------------------------------------------

    private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
        final String actorAddress = PekkoUtils.getRpcURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        return Tuple2.of(actorAddress, hostname);
    }

    private <C extends RpcGateway> CompletableFuture<C> connectInternal(
            final String address,
            final Class<C> clazz,
            Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
        checkState(!stopped, "RpcService is stopped");

        LOG.debug(
                "Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
                address,
                clazz.getName());

        final CompletableFuture<ActorRef> actorRefFuture = resolveActorAddress(address);

        final CompletableFuture<HandshakeSuccessMessage> handshakeFuture =
                actorRefFuture.thenCompose(
                        (ActorRef actorRef) ->
                                ScalaFutureUtils.toJava(
                                        Patterns.ask(
                                                        actorRef,
                                                        new RemoteHandshakeMessage(
                                                                clazz, getVersion()),
                                                        configuration.getTimeout().toMillis())
                                                .<HandshakeSuccessMessage>mapTo(
                                                        ClassTag$.MODULE$
                                                                .<HandshakeSuccessMessage>apply(
                                                                        HandshakeSuccessMessage
                                                                                .class))));

        final CompletableFuture<C> gatewayFuture =
                actorRefFuture.thenCombineAsync(
                        handshakeFuture,
                        (ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
                            InvocationHandler invocationHandler =
                                    invocationHandlerFactory.apply(actorRef);

                            // Rather than using the System ClassLoader directly, we derive the
                            // ClassLoader from this class.
                            // That works better in cases where Flink runs embedded and
                            // all Flink code is loaded dynamically
                            // (for example from an OSGI bundle) through a custom ClassLoader
                            ClassLoader classLoader = getClass().getClassLoader();

                            @SuppressWarnings("unchecked")
                            C proxy =
                                    (C)
                                            Proxy.newProxyInstance(
                                                    classLoader,
                                                    new Class<?>[] {clazz},
                                                    invocationHandler);

                            return proxy;
                        },
                        actorSystem.dispatcher());

        return guardCompletionWithContextClassLoader(gatewayFuture, flinkClassLoader);
    }

    private CompletableFuture<ActorRef> resolveActorAddress(String address) {
        final ActorSelection actorSel = actorSystem.actorSelection(address);

        return actorSel.resolveOne(configuration.getTimeout())
                .toCompletableFuture()
                .exceptionally(
                        error -> {
                            throw new CompletionException(
                                    new RpcConnectionException(
                                            String.format(
                                                    "Could not connect to rpc endpoint under address %s.",
                                                    address),
                                            error));
                        });
    }

    // ---------------------------------------------------------------------------------------
    // Private inner classes
    // ---------------------------------------------------------------------------------------

    private static final class Supervisor implements AutoCloseableAsync {

        private final ActorRef actor;

        private final ExecutorService terminationFutureExecutor;

        private Supervisor(ActorRef actor, ExecutorService terminationFutureExecutor) {
            this.actor = actor;
            this.terminationFutureExecutor = terminationFutureExecutor;
        }

        private static Supervisor create(
                ActorRef actorRef, ExecutorService terminationFutureExecutor) {
            return new Supervisor(actorRef, terminationFutureExecutor);
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return ExecutorUtils.nonBlockingShutdown(
                    30L, TimeUnit.SECONDS, terminationFutureExecutor);
        }
    }
}
