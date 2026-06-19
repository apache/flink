/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.rpc.pekko.exceptions.UnknownMessageException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.pekko.PekkoException;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.ChildRestartStats;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.actor.SupervisorStrategy;
import org.apache.pekko.japi.pf.DeciderBuilder;
import org.apache.pekko.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import scala.PartialFunction;
import scala.collection.Iterable;

/**
 * Supervisor actor which is responsible for starting {@link PekkoRpcActor} instances and monitoring
 * when the actors have terminated.
 */
class SupervisorActor extends AbstractActor {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorActor.class);

    private final Executor terminationFutureExecutor;

    private final Map<ActorRef, RpcActorRegistration> registeredRpcActors;

    SupervisorActor(Executor terminationFutureExecutor) {
        this.terminationFutureExecutor = terminationFutureExecutor;
        this.registeredRpcActors = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartRpcActor.class, this::createStartRpcActorMessage)
                .matchAny(this::handleUnknownMessage)
                .build();
    }

    @Override
    public void postStop() throws Exception {
        LOG.debug("Stopping supervisor actor.");

        super.postStop();

        for (RpcActorRegistration actorRegistration : registeredRpcActors.values()) {
            terminateRpcActorOnStop(actorRegistration);
        }

        registeredRpcActors.clear();
    }

    @Override
    public SupervisorActorSupervisorStrategy supervisorStrategy() {
        return new SupervisorActorSupervisorStrategy();
    }

    private void terminateRpcActorOnStop(RpcActorRegistration rpcActorRegistration) {
        rpcActorRegistration.terminateExceptionally(
                new RpcException(
                        String.format(
                                "Unexpected closing of %s with name %s.",
                                getClass().getSimpleName(), rpcActorRegistration.getEndpointId())),
                terminationFutureExecutor);
    }

    private void createStartRpcActorMessage(StartRpcActor startRpcActor) {
        final String endpointId = startRpcActor.getEndpointId();
        final RpcActorRegistration rpcActorRegistration = new RpcActorRegistration(endpointId);

        final Props rpcActorProps =
                startRpcActor
                        .getPropsFactory()
                        .create(rpcActorRegistration.getInternalTerminationFuture());

        LOG.debug(
                "Starting {} with name {}.",
                rpcActorProps.actorClass().getSimpleName(),
                endpointId);

        try {
            final ActorRef actorRef = getContext().actorOf(rpcActorProps, endpointId);

            registeredRpcActors.put(actorRef, rpcActorRegistration);

            getSender()
                    .tell(
                            StartRpcActorResponse.success(
                                    ActorRegistration.create(
                                            actorRef,
                                            rpcActorRegistration.getExternalTerminationFuture())),
                            getSelf());
        } catch (PekkoException e) {
            getSender().tell(StartRpcActorResponse.failure(e), getSelf());
        }
    }

    private void rpcActorTerminated(ActorRef actorRef) {
        final RpcActorRegistration actorRegistration = removeAkkaRpcActor(actorRef);

        LOG.debug("RpcActor {} has terminated.", actorRef.path());
        actorRegistration.terminate(terminationFutureExecutor);
    }

    private void rpcActorFailed(ActorRef actorRef, Throwable cause) {
        LOG.warn("RpcActor {} has failed. Shutting it down now.", actorRef.path(), cause);

        for (Map.Entry<ActorRef, RpcActorRegistration> registeredRpcActor :
                registeredRpcActors.entrySet()) {
            final ActorRef otherActorRef = registeredRpcActor.getKey();
            if (otherActorRef.equals(actorRef)) {
                final RpcException error =
                        new RpcException(
                                String.format(
                                        "Stopping actor %s because it failed.", actorRef.path()),
                                cause);
                registeredRpcActor.getValue().markFailed(error);
            } else {
                final RpcException siblingException =
                        new RpcException(
                                String.format(
                                        "Stopping actor %s because its sibling %s has failed.",
                                        otherActorRef.path(), actorRef.path()));
                registeredRpcActor.getValue().markFailed(siblingException);
            }
        }

        getContext().getSystem().terminate();
    }

    private RpcActorRegistration removeAkkaRpcActor(ActorRef actorRef) {
        return Optional.ofNullable(registeredRpcActors.remove(actorRef))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        String.format(
                                                "Could not find actor %s.", actorRef.path())));
    }

    private void handleUnknownMessage(Object msg) {
        final UnknownMessageException cause =
                new UnknownMessageException(
                        String.format("Cannot handle unknown message %s.", msg));
        getSender().tell(new Status.Failure(cause), getSelf());
        throw cause;
    }

    public static String getActorName() {
        return PekkoRpcServiceUtils.SUPERVISOR_NAME;
    }

    public static ActorRef startSupervisorActor(
            ActorSystem actorSystem, Executor terminationFutureExecutor) {
        final Props supervisorProps =
                Props.create(SupervisorActor.class, terminationFutureExecutor)
                        .withDispatcher("pekko.actor.supervisor-dispatcher");
        return actorSystem.actorOf(supervisorProps, getActorName());
    }

    public static StartRpcActorResponse startRpcActor(
            ActorRef supervisor, StartRpcActor.PropsFactory propsFactory, String endpointId) {
        return Patterns.ask(
                        supervisor,
                        createStartRpcActorMessage(propsFactory, endpointId),
                        RpcUtils.INF_DURATION)
                .toCompletableFuture()
                .thenApply(StartRpcActorResponse.class::cast)
                .join();
    }

    public static StartRpcActor createStartRpcActorMessage(
            StartRpcActor.PropsFactory propsFactory, String endpointId) {
        return StartRpcActor.create(propsFactory, endpointId);
    }

    // -----------------------------------------------------------------------------
    // Internal classes
    // -----------------------------------------------------------------------------

    private final class SupervisorActorSupervisorStrategy extends SupervisorStrategy {

        @Override
        public PartialFunction<Throwable, Directive> decider() {
            return DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.stop()).build();
        }

        @Override
        public boolean loggingEnabled() {
            return false;
        }

        @Override
        public void handleChildTerminated(
                org.apache.pekko.actor.ActorContext context,
                ActorRef child,
                Iterable<ActorRef> children) {
            rpcActorTerminated(child);
        }

        @Override
        public void processFailure(
                org.apache.pekko.actor.ActorContext context,
                boolean restart,
                ActorRef child,
                Throwable cause,
                ChildRestartStats stats,
                Iterable<ChildRestartStats> children) {
            Preconditions.checkArgument(
                    !restart, "The supervisor strategy should never restart an actor.");

            rpcActorFailed(child, cause);
        }
    }

    private static final class RpcActorRegistration {
        private final String endpointId;

        private final CompletableFuture<Void> internalTerminationFuture;

        private final CompletableFuture<Void> externalTerminationFuture;

        @Nullable private Throwable errorCause;

        private RpcActorRegistration(String endpointId) {
            this.endpointId = endpointId;
            internalTerminationFuture = new CompletableFuture<>();
            externalTerminationFuture = new CompletableFuture<>();
            errorCause = null;
        }

        private CompletableFuture<Void> getInternalTerminationFuture() {
            return internalTerminationFuture;
        }

        private CompletableFuture<Void> getExternalTerminationFuture() {
            return externalTerminationFuture;
        }

        private String getEndpointId() {
            return endpointId;
        }

        private void terminate(Executor terminationFutureExecutor) {
            CompletableFuture<Void> terminationFuture = internalTerminationFuture;

            if (errorCause != null) {
                if (!internalTerminationFuture.completeExceptionally(errorCause)) {
                    // we have another failure reason -> let's add it
                    terminationFuture =
                            internalTerminationFuture.handle(
                                    (ignored, throwable) -> {
                                        if (throwable != null) {
                                            errorCause.addSuppressed(throwable);
                                        }

                                        throw new CompletionException(errorCause);
                                    });
                }
            } else {
                internalTerminationFuture.completeExceptionally(
                        new RpcException(
                                String.format(
                                        "RpcEndpoint %s did not complete the internal termination future.",
                                        endpointId)));
            }

            FutureUtils.forwardAsync(
                    terminationFuture, externalTerminationFuture, terminationFutureExecutor);
        }

        private void terminateExceptionally(Throwable cause, Executor terminationFutureExecutor) {
            terminationFutureExecutor.execute(
                    () -> externalTerminationFuture.completeExceptionally(cause));
        }

        public void markFailed(Throwable cause) {
            if (errorCause == null) {
                errorCause = cause;
            } else {
                errorCause.addSuppressed(cause);
            }
        }
    }

    // -----------------------------------------------------------------------------
    // Messages
    // -----------------------------------------------------------------------------

    static final class StartRpcActor {
        private final PropsFactory propsFactory;
        private final String endpointId;

        private StartRpcActor(PropsFactory propsFactory, String endpointId) {
            this.propsFactory = propsFactory;
            this.endpointId = endpointId;
        }

        public String getEndpointId() {
            return endpointId;
        }

        public PropsFactory getPropsFactory() {
            return propsFactory;
        }

        private static StartRpcActor create(PropsFactory propsFactory, String endpointId) {
            return new StartRpcActor(propsFactory, endpointId);
        }

        interface PropsFactory {
            Props create(CompletableFuture<Void> terminationFuture);
        }
    }

    static final class ActorRegistration {
        private final ActorRef actorRef;
        private final CompletableFuture<Void> terminationFuture;

        private ActorRegistration(ActorRef actorRef, CompletableFuture<Void> terminationFuture) {
            this.actorRef = actorRef;
            this.terminationFuture = terminationFuture;
        }

        public ActorRef getActorRef() {
            return actorRef;
        }

        public CompletableFuture<Void> getTerminationFuture() {
            return terminationFuture;
        }

        public static ActorRegistration create(
                ActorRef actorRef, CompletableFuture<Void> terminationFuture) {
            return new ActorRegistration(actorRef, terminationFuture);
        }
    }

    static final class StartRpcActorResponse {
        @Nullable private final ActorRegistration actorRegistration;

        @Nullable private final Throwable error;

        private StartRpcActorResponse(
                @Nullable ActorRegistration actorRegistration, @Nullable Throwable error) {
            this.actorRegistration = actorRegistration;
            this.error = error;
        }

        public <X extends Throwable> ActorRegistration orElseThrow(
                Function<? super Throwable, ? extends X> throwableFunction) throws X {
            if (actorRegistration != null) {
                return actorRegistration;
            } else {
                throw throwableFunction.apply(error);
            }
        }

        public static StartRpcActorResponse success(ActorRegistration actorRegistration) {
            return new StartRpcActorResponse(actorRegistration, null);
        }

        public static StartRpcActorResponse failure(Throwable error) {
            return new StartRpcActorResponse(null, error);
        }
    }
}
