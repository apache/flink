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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.concurrent.akka.AkkaFutureUtils;
import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcRuntimeException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.function.FunctionUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.DeadLetter;
import akka.actor.Props;
import akka.pattern.Patterns;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.reflect.ClassTag$;

import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.guardCompletionWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.runWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.akka.ClassLoadingUtils.withContextClassLoader;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive RPC
 * invocations from a {@link RpcGateway}.
 */
@ThreadSafe
public class AkkaRpcService implements RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

    static final int VERSION = 2;

    private final Object lock = new Object();

    private final ActorSystem actorSystem;
    private final AkkaRpcServiceConfiguration configuration;

    private final ClassLoader flinkClassLoader;

    @GuardedBy("lock")
    private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

    private final String address;
    private final int port;

    private final boolean captureAskCallstacks;

    private final ScheduledExecutor internalScheduledExecutor;

    private final CompletableFuture<Void> terminationFuture;

    private final Supervisor supervisor;

    private volatile boolean stopped;

    @VisibleForTesting
    public AkkaRpcService(
            final ActorSystem actorSystem, final AkkaRpcServiceConfiguration configuration) {
        this(actorSystem, configuration, AkkaRpcService.class.getClassLoader());
    }

    AkkaRpcService(
            final ActorSystem actorSystem,
            final AkkaRpcServiceConfiguration configuration,
            final ClassLoader flinkClassLoader) {
        this.actorSystem = checkNotNull(actorSystem, "actor system");
        this.configuration = checkNotNull(configuration, "akka rpc service configuration");
        this.flinkClassLoader = checkNotNull(flinkClassLoader, "flinkClassLoader");

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

        captureAskCallstacks = configuration.captureAskCallStack();

        // Akka always sets the threads context class loader to the class loader with which it was
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
                                "AkkaRpcService-Supervisor-Termination-Future-Executor"));
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

    // this method does not mutate state and is thus thread-safe
    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(
            final String address, final Class<C> clazz) {

        return connectInternal(
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

                    return new AkkaInvocationHandler(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
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

                    return new FencedAkkaInvocationHandler<>(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        checkNotNull(rpcEndpoint, "rpc endpoint");

        final SupervisorActor.ActorRegistration actorRegistration =
                registerAkkaRpcActor(rpcEndpoint);
        final ActorRef actorRef = actorRegistration.getActorRef();
        final CompletableFuture<Void> actorTerminationFuture =
                actorRegistration.getTerminationFuture();

        LOG.info(
                "Starting RPC endpoint for {} at {} .",
                rpcEndpoint.getClass().getName(),
                actorRef.path());

        final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
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
        implementedRpcGateways.add(AkkaBasedEndpoint.class);

        final InvocationHandler akkaInvocationHandler;

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            // a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
            akkaInvocationHandler =
                    new FencedAkkaInvocationHandler<>(
                            akkaAddress,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            actorTerminationFuture,
                            ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);

            implementedRpcGateways.add(FencedMainThreadExecutable.class);
        } else {
            akkaInvocationHandler =
                    new AkkaInvocationHandler(
                            akkaAddress,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
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
                                akkaInvocationHandler);

        return server;
    }

    private <C extends RpcEndpoint & RpcGateway>
            SupervisorActor.ActorRegistration registerAkkaRpcActor(C rpcEndpoint) {
        final Class<? extends AbstractActor> akkaRpcActorType;

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            akkaRpcActorType = FencedAkkaRpcActor.class;
        } else {
            akkaRpcActorType = AkkaRpcActor.class;
        }

        synchronized (lock) {
            checkState(!stopped, "RpcService is stopped");

            final SupervisorActor.StartAkkaRpcActorResponse startAkkaRpcActorResponse =
                    SupervisorActor.startAkkaRpcActor(
                            supervisor.getActor(),
                            actorTerminationFuture ->
                                    Props.create(
                                            akkaRpcActorType,
                                            rpcEndpoint,
                                            actorTerminationFuture,
                                            getVersion(),
                                            configuration.getMaximumFramesize(),
                                            flinkClassLoader),
                            rpcEndpoint.getEndpointId());

            final SupervisorActor.ActorRegistration actorRegistration =
                    startAkkaRpcActorResponse.orElseThrow(
                            cause ->
                                    new AkkaRpcRuntimeException(
                                            String.format(
                                                    "Could not create the %s for %s.",
                                                    AkkaRpcActor.class.getSimpleName(),
                                                    rpcEndpoint.getEndpointId()),
                                            cause));

            actors.put(actorRegistration.getActorRef(), rpcEndpoint);

            return actorRegistration;
        }
    }

    @Override
    public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
        if (rpcServer instanceof AkkaBasedEndpoint) {

            InvocationHandler fencedInvocationHandler =
                    new FencedAkkaInvocationHandler<>(
                            rpcServer.getAddress(),
                            rpcServer.getHostname(),
                            ((AkkaBasedEndpoint) rpcServer).getActorRef(),
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);

            // Rather than using the System ClassLoader directly, we derive the ClassLoader
            // from this class . That works better in cases where Flink runs embedded and all Flink
            // code is loaded dynamically (for example from an OSGI bundle) through a custom
            // ClassLoader
            ClassLoader classLoader = getClass().getClassLoader();

            return (RpcServer)
                    Proxy.newProxyInstance(
                            classLoader,
                            new Class<?>[] {RpcServer.class, AkkaBasedEndpoint.class},
                            fencedInvocationHandler);
        } else {
            throw new RuntimeException(
                    "The given RpcServer must implement the AkkaGateway in order to fence it.");
        }
    }

    @Override
    public void stopServer(RpcServer selfGateway) {
        if (selfGateway instanceof AkkaBasedEndpoint) {
            final AkkaBasedEndpoint akkaClient = (AkkaBasedEndpoint) selfGateway;
            final RpcEndpoint rpcEndpoint;

            synchronized (lock) {
                if (stopped) {
                    return;
                } else {
                    rpcEndpoint = actors.remove(akkaClient.getActorRef());
                }
            }

            if (rpcEndpoint != null) {
                terminateAkkaRpcActor(akkaClient.getActorRef(), rpcEndpoint);
            } else {
                LOG.debug(
                        "RPC endpoint {} already stopped or from different RPC service",
                        selfGateway.getAddress());
            }
        }
    }

    @Override
    public CompletableFuture<Void> stopService() {
        final CompletableFuture<Void> akkaRpcActorsTerminationFuture;

        synchronized (lock) {
            if (stopped) {
                return terminationFuture;
            }

            LOG.info("Stopping Akka RPC service.");

            stopped = true;

            akkaRpcActorsTerminationFuture = terminateAkkaRpcActors();
        }

        final CompletableFuture<Void> supervisorTerminationFuture =
                FutureUtils.composeAfterwards(
                        akkaRpcActorsTerminationFuture, supervisor::closeAsync);

        final CompletableFuture<Void> actorSystemTerminationFuture =
                FutureUtils.composeAfterwards(
                        supervisorTerminationFuture,
                        () -> AkkaFutureUtils.toJava(actorSystem.terminate()));

        actorSystemTerminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> {
                    runWithContextClassLoader(
                            () -> FutureUtils.doForward(ignored, throwable, terminationFuture),
                            flinkClassLoader);

                    LOG.info("Stopped Akka RPC service.");
                });

        return terminationFuture;
    }

    @GuardedBy("lock")
    @Nonnull
    private CompletableFuture<Void> terminateAkkaRpcActors() {
        final Collection<CompletableFuture<Void>> akkaRpcActorTerminationFutures =
                new ArrayList<>(actors.size());

        for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
            akkaRpcActorTerminationFutures.add(
                    terminateAkkaRpcActor(
                            actorRefRpcEndpointEntry.getKey(),
                            actorRefRpcEndpointEntry.getValue()));
        }
        actors.clear();

        return FutureUtils.waitForAll(akkaRpcActorTerminationFutures);
    }

    private CompletableFuture<Void> terminateAkkaRpcActor(
            ActorRef akkaRpcActorRef, RpcEndpoint rpcEndpoint) {
        akkaRpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

        return rpcEndpoint.getTerminationFuture();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
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
        getScheduledExecutor().execute(runnable);
    }

    @Override
    public <T> CompletableFuture<T> execute(Callable<T> callable) {
        return CompletableFuture.supplyAsync(
                FunctionUtils.uncheckedSupplier(callable::call), getScheduledExecutor());
    }

    // ---------------------------------------------------------------------------------------
    // Private helper methods
    // ---------------------------------------------------------------------------------------

    private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
        final String actorAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
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
                                AkkaFutureUtils.toJava(
                                        Patterns.ask(
                                                        actorRef,
                                                        new RemoteHandshakeMessage(
                                                                clazz, getVersion()),
                                                        configuration.getTimeout().toMilliseconds())
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

        return actorSel.resolveOne(TimeUtils.toDuration(configuration.getTimeout()))
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
