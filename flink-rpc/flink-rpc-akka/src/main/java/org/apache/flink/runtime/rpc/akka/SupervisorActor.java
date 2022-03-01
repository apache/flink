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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaRpcException;
import org.apache.flink.runtime.rpc.akka.exceptions.AkkaUnknownMessageException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import akka.AkkaException;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.pattern.Patterns;
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
 * Supervisor actor which is responsible for starting {@link AkkaRpcActor} instances and monitoring
 * when the actors have terminated.
 */
class SupervisorActor extends AbstractActor {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorActor.class);

    private final Executor terminationFutureExecutor;

    private final Map<ActorRef, AkkaRpcActorRegistration> registeredAkkaRpcActors;

    SupervisorActor(Executor terminationFutureExecutor) {
        this.terminationFutureExecutor = terminationFutureExecutor;
        this.registeredAkkaRpcActors = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartAkkaRpcActor.class, this::createStartAkkaRpcActorMessage)
                .matchAny(this::handleUnknownMessage)
                .build();
    }

    @Override
    public void postStop() throws Exception {
        LOG.debug("Stopping supervisor actor.");

        super.postStop();

        for (AkkaRpcActorRegistration actorRegistration : registeredAkkaRpcActors.values()) {
            terminateAkkaRpcActorOnStop(actorRegistration);
        }

        registeredAkkaRpcActors.clear();
    }

    @Override
    public SupervisorActorSupervisorStrategy supervisorStrategy() {
        return new SupervisorActorSupervisorStrategy();
    }

    private void terminateAkkaRpcActorOnStop(AkkaRpcActorRegistration akkaRpcActorRegistration) {
        akkaRpcActorRegistration.terminateExceptionally(
                new AkkaRpcException(
                        String.format(
                                "Unexpected closing of %s with name %s.",
                                getClass().getSimpleName(),
                                akkaRpcActorRegistration.getEndpointId())),
                terminationFutureExecutor);
    }

    private void createStartAkkaRpcActorMessage(StartAkkaRpcActor startAkkaRpcActor) {
        final String endpointId = startAkkaRpcActor.getEndpointId();
        final AkkaRpcActorRegistration akkaRpcActorRegistration =
                new AkkaRpcActorRegistration(endpointId);

        final Props akkaRpcActorProps =
                startAkkaRpcActor
                        .getPropsFactory()
                        .create(akkaRpcActorRegistration.getInternalTerminationFuture());

        LOG.debug(
                "Starting {} with name {}.",
                akkaRpcActorProps.actorClass().getSimpleName(),
                endpointId);

        try {
            final ActorRef actorRef = getContext().actorOf(akkaRpcActorProps, endpointId);

            registeredAkkaRpcActors.put(actorRef, akkaRpcActorRegistration);

            getSender()
                    .tell(
                            StartAkkaRpcActorResponse.success(
                                    ActorRegistration.create(
                                            actorRef,
                                            akkaRpcActorRegistration
                                                    .getExternalTerminationFuture())),
                            getSelf());
        } catch (AkkaException akkaException) {
            getSender().tell(StartAkkaRpcActorResponse.failure(akkaException), getSelf());
        }
    }

    private void akkaRpcActorTerminated(ActorRef actorRef) {
        final AkkaRpcActorRegistration actorRegistration = removeAkkaRpcActor(actorRef);

        LOG.debug("AkkaRpcActor {} has terminated.", actorRef.path());
        actorRegistration.terminate(terminationFutureExecutor);
    }

    private void akkaRpcActorFailed(ActorRef actorRef, Throwable cause) {
        LOG.warn("AkkaRpcActor {} has failed. Shutting it down now.", actorRef.path(), cause);

        for (Map.Entry<ActorRef, AkkaRpcActorRegistration> registeredAkkaRpcActor :
                registeredAkkaRpcActors.entrySet()) {
            final ActorRef otherActorRef = registeredAkkaRpcActor.getKey();
            if (otherActorRef.equals(actorRef)) {
                final AkkaRpcException error =
                        new AkkaRpcException(
                                String.format(
                                        "Stopping actor %s because it failed.", actorRef.path()),
                                cause);
                registeredAkkaRpcActor.getValue().markFailed(error);
            } else {
                final AkkaRpcException siblingException =
                        new AkkaRpcException(
                                String.format(
                                        "Stopping actor %s because its sibling %s has failed.",
                                        otherActorRef.path(), actorRef.path()));
                registeredAkkaRpcActor.getValue().markFailed(siblingException);
            }
        }

        getContext().getSystem().terminate();
    }

    private AkkaRpcActorRegistration removeAkkaRpcActor(ActorRef actorRef) {
        return Optional.ofNullable(registeredAkkaRpcActors.remove(actorRef))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        String.format(
                                                "Could not find actor %s.", actorRef.path())));
    }

    private void handleUnknownMessage(Object msg) {
        final AkkaUnknownMessageException cause =
                new AkkaUnknownMessageException(
                        String.format("Cannot handle unknown message %s.", msg));
        getSender().tell(new akka.actor.Status.Failure(cause), getSelf());
        throw cause;
    }

    public static String getActorName() {
        return AkkaRpcServiceUtils.SUPERVISOR_NAME;
    }

    public static ActorRef startSupervisorActor(
            ActorSystem actorSystem, Executor terminationFutureExecutor) {
        final Props supervisorProps =
                Props.create(SupervisorActor.class, terminationFutureExecutor)
                        .withDispatcher("akka.actor.supervisor-dispatcher");
        return actorSystem.actorOf(supervisorProps, getActorName());
    }

    public static StartAkkaRpcActorResponse startAkkaRpcActor(
            ActorRef supervisor, StartAkkaRpcActor.PropsFactory propsFactory, String endpointId) {
        return Patterns.ask(
                        supervisor,
                        createStartAkkaRpcActorMessage(propsFactory, endpointId),
                        RpcUtils.INF_DURATION)
                .toCompletableFuture()
                .thenApply(SupervisorActor.StartAkkaRpcActorResponse.class::cast)
                .join();
    }

    public static StartAkkaRpcActor createStartAkkaRpcActorMessage(
            StartAkkaRpcActor.PropsFactory propsFactory, String endpointId) {
        return StartAkkaRpcActor.create(propsFactory, endpointId);
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
                akka.actor.ActorContext context, ActorRef child, Iterable<ActorRef> children) {
            akkaRpcActorTerminated(child);
        }

        @Override
        public void processFailure(
                akka.actor.ActorContext context,
                boolean restart,
                ActorRef child,
                Throwable cause,
                ChildRestartStats stats,
                Iterable<ChildRestartStats> children) {
            Preconditions.checkArgument(
                    !restart, "The supervisor strategy should never restart an actor.");

            akkaRpcActorFailed(child, cause);
        }
    }

    private static final class AkkaRpcActorRegistration {
        private final String endpointId;

        private final CompletableFuture<Void> internalTerminationFuture;

        private final CompletableFuture<Void> externalTerminationFuture;

        @Nullable private Throwable errorCause;

        private AkkaRpcActorRegistration(String endpointId) {
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
                        new AkkaRpcException(
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

    static final class StartAkkaRpcActor {
        private final PropsFactory propsFactory;
        private final String endpointId;

        private StartAkkaRpcActor(PropsFactory propsFactory, String endpointId) {
            this.propsFactory = propsFactory;
            this.endpointId = endpointId;
        }

        public String getEndpointId() {
            return endpointId;
        }

        public PropsFactory getPropsFactory() {
            return propsFactory;
        }

        private static StartAkkaRpcActor create(PropsFactory propsFactory, String endpointId) {
            return new StartAkkaRpcActor(propsFactory, endpointId);
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

    static final class StartAkkaRpcActorResponse {
        @Nullable private final ActorRegistration actorRegistration;

        @Nullable private final Throwable error;

        private StartAkkaRpcActorResponse(
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

        public static StartAkkaRpcActorResponse success(ActorRegistration actorRegistration) {
            return new StartAkkaRpcActorResponse(actorRegistration, null);
        }

        public static StartAkkaRpcActorResponse failure(Throwable error) {
            return new StartAkkaRpcActorResponse(null, error);
        }
    }
}
