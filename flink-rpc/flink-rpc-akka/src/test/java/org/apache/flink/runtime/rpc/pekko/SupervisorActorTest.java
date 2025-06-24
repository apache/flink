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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Terminated;
import org.apache.pekko.japi.pf.ReceiveBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link SupervisorActor}. */
class SupervisorActorTest {

    @RegisterExtension
    private final ActorSystemExtension actorSystemExtension =
            ActorSystemExtension.defaultConfiguration();

    @Test
    void completesTerminationFutureIfActorStops() {
        final ActorSystem actorSystem = actorSystemExtension.getActorSystem();

        final ActorRef supervisor =
                SupervisorActor.startSupervisorActor(actorSystem, actorSystem.getDispatcher());

        final SupervisorActor.ActorRegistration actorRegistration =
                startRpcActor(supervisor, "foobar");

        final CompletableFuture<Void> terminationFuture = actorRegistration.getTerminationFuture();
        assertThat(terminationFuture).isNotDone();

        actorRegistration
                .getActorRef()
                .tell(TerminateWithFutureCompletion.normal(), ActorRef.noSender());

        terminationFuture.join();
    }

    @Test
    void completesTerminationFutureExceptionallyIfActorStopsExceptionally() throws Exception {
        final ActorSystem actorSystem = actorSystemExtension.getActorSystem();

        final ActorRef supervisor =
                SupervisorActor.startSupervisorActor(actorSystem, actorSystem.getDispatcher());

        final SupervisorActor.ActorRegistration actorRegistration =
                startRpcActor(supervisor, "foobar");

        final CompletableFuture<Void> terminationFuture = actorRegistration.getTerminationFuture();
        assertThat(terminationFuture).isNotDone();

        final FlinkException cause = new FlinkException("Test cause.");
        actorRegistration
                .getActorRef()
                .tell(TerminateWithFutureCompletion.exceptionally(cause), ActorRef.noSender());

        try {
            terminationFuture.get();
            fail("Expected the termination future being completed exceptionally");
        } catch (ExecutionException expected) {
            ExceptionUtils.findThrowable(expected, e -> e.equals(cause))
                    .orElseThrow(() -> new FlinkException("Unexpected exception", expected));
        }
    }

    @Test
    void completesTerminationFutureExceptionallyIfActorStopsWithoutReason()
            throws InterruptedException {
        final ActorSystem actorSystem = actorSystemExtension.getActorSystem();

        final ActorRef supervisor =
                SupervisorActor.startSupervisorActor(actorSystem, actorSystem.getDispatcher());

        final SupervisorActor.ActorRegistration actorRegistration =
                startRpcActor(supervisor, "foobar");

        final CompletableFuture<Void> terminationFuture = actorRegistration.getTerminationFuture();
        assertThat(terminationFuture).isNotDone();

        actorRegistration.getActorRef().tell(Terminate.INSTANCE, ActorRef.noSender());

        try {
            terminationFuture.get();
            fail("Expected the termination future being completed exceptionally");
        } catch (ExecutionException expected) {
        }
    }

    @Test
    void completesTerminationFutureExceptionallyIfActorFails() throws Exception {
        final ActorSystem actorSystem = actorSystemExtension.getActorSystem();

        final ActorRef supervisor =
                SupervisorActor.startSupervisorActor(actorSystem, actorSystem.getDispatcher());

        final SupervisorActor.ActorRegistration actorRegistration =
                startRpcActor(supervisor, "foobar");

        final CompletableFuture<Void> terminationFuture = actorRegistration.getTerminationFuture();
        assertThat(terminationFuture).isNotDone();

        final CompletableFuture<Terminated> actorSystemTerminationFuture =
                actorSystem.getWhenTerminated().toCompletableFuture();

        final FlinkException cause = new FlinkException("Test cause.");
        actorRegistration.getActorRef().tell(Fail.exceptionally(cause), ActorRef.noSender());

        try {
            terminationFuture.get();
            fail("Expected the termination future being completed exceptionally");
        } catch (ExecutionException expected) {
            ExceptionUtils.findThrowable(expected, e -> e.equals(cause))
                    .orElseThrow(() -> new FlinkException("Unexpected exception", expected));
        }

        // make sure that the supervisor actor has stopped --> terminating the actor system
        actorSystemTerminationFuture.join();
    }

    @Test
    void completesTerminationFutureOfSiblingsIfActorFails() throws Exception {
        final ActorSystem actorSystem = actorSystemExtension.getActorSystem();

        final ActorRef supervisor =
                SupervisorActor.startSupervisorActor(actorSystem, actorSystem.getDispatcher());

        final SupervisorActor.ActorRegistration actorRegistration1 =
                startRpcActor(supervisor, "foobar1");
        final SupervisorActor.ActorRegistration actorRegistration2 =
                startRpcActor(supervisor, "foobar2");

        final CompletableFuture<Void> terminationFuture = actorRegistration2.getTerminationFuture();
        assertThat(terminationFuture).isNotDone();

        final FlinkException cause = new FlinkException("Test cause.");
        actorRegistration1.getActorRef().tell(Fail.exceptionally(cause), ActorRef.noSender());

        try {
            terminationFuture.get();
            fail("Expected the termination future being completed exceptionally");
        } catch (ExecutionException expected) {
        }
    }

    private SupervisorActor.ActorRegistration startRpcActor(
            ActorRef supervisor, String endpointId) {
        final SupervisorActor.StartRpcActorResponse startResponse =
                SupervisorActor.startRpcActor(
                        supervisor,
                        terminationFuture -> Props.create(SimpleActor.class, terminationFuture),
                        endpointId);

        return startResponse.orElseThrow(
                cause -> new AssertionError("Expected the start to succeed.", cause));
    }

    private static final class SimpleActor extends AbstractActor {

        private final CompletableFuture<Void> terminationFuture;

        private SimpleActor(CompletableFuture<Void> terminationFuture) {
            this.terminationFuture = terminationFuture;
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .match(Terminate.class, this::terminate)
                    .match(
                            TerminateWithFutureCompletion.class,
                            this::terminateActorWithFutureCompletion)
                    .match(Fail.class, this::fail)
                    .build();
        }

        private void fail(Fail fail) {
            throw new RuntimeException(fail.getCause());
        }

        private void terminate(Terminate terminate) {
            terminateActor();
        }

        private void terminateActor() {
            getContext().stop(getSelf());
        }

        private void terminateActorWithFutureCompletion(
                TerminateWithFutureCompletion terminateWithFutureCompletion) {
            final Throwable terminationError = terminateWithFutureCompletion.getTerminationError();
            if (terminationError == null) {
                terminationFuture.complete(null);
            } else {
                terminationFuture.completeExceptionally(terminationError);
            }

            terminateActor();
        }
    }

    private static final class Terminate {
        private static final Terminate INSTANCE = new Terminate();
    }

    private static final class TerminateWithFutureCompletion {
        @Nullable private final Throwable terminationError;

        private TerminateWithFutureCompletion(@Nullable Throwable terminationError) {
            this.terminationError = terminationError;
        }

        @Nullable
        private Throwable getTerminationError() {
            return terminationError;
        }

        private static TerminateWithFutureCompletion normal() {
            return new TerminateWithFutureCompletion(null);
        }

        private static TerminateWithFutureCompletion exceptionally(Throwable cause) {
            return new TerminateWithFutureCompletion(cause);
        }
    }

    private static final class Fail {
        private final Throwable cause;

        private Fail(Throwable cause) {
            this.cause = cause;
        }

        private Throwable getCause() {
            return cause;
        }

        private static Fail exceptionally(Throwable cause) {
            return new Fail(cause);
        }
    }
}
