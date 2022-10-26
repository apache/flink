/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.util.concurrent.TestingUncaughtExceptionHandler;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RobustActorSystem}. */
class RobustActorSystemTest {

    private RobustActorSystem robustActorSystem = null;
    private TestingUncaughtExceptionHandler testingUncaughtExceptionHandler = null;

    @BeforeEach
    void setup() {
        testingUncaughtExceptionHandler = new TestingUncaughtExceptionHandler();
        robustActorSystem =
                RobustActorSystem.create(
                        "testSystem",
                        AkkaUtils.getForkJoinExecutorConfig(
                                RpcUtils.getTestForkJoinExecutorConfiguration()),
                        testingUncaughtExceptionHandler);
    }

    @AfterEach
    void teardown() {
        robustActorSystem.terminate();
        testingUncaughtExceptionHandler = null;
    }

    @Test
    void testUncaughtExceptionHandler() {
        final Error error = new UnknownError("Foobar");

        robustActorSystem
                .dispatcher()
                .execute(
                        () -> {
                            throw error;
                        });

        final Throwable uncaughtException =
                testingUncaughtExceptionHandler.waitForUncaughtException();

        assertThat(uncaughtException).isSameAs(error);
    }

    @Test
    void testUncaughtExceptionHandlerFromActor() {
        final Error error = new UnknownError();
        final ActorRef actor =
                robustActorSystem.actorOf(Props.create(UncaughtExceptionActor.class, error));

        actor.tell(new Failure(), null);

        final Throwable uncaughtException =
                testingUncaughtExceptionHandler.waitForUncaughtException();

        assertThat(uncaughtException).isSameAs(error);
    }

    private static class UncaughtExceptionActor extends AbstractActor {
        private final Error failure;

        public UncaughtExceptionActor(Error failure) {
            this.failure = failure;
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .match(
                            Failure.class,
                            ignored -> {
                                throw failure;
                            })
                    .build();
        }
    }

    private static class Failure {}
}
