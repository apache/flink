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
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.TestingUncaughtExceptionHandler;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/** Tests for the {@link RobustActorSystem}. */
public class RobustActorSystemTest extends TestLogger {

    private RobustActorSystem robustActorSystem = null;
    private TestingUncaughtExceptionHandler testingUncaughtExceptionHandler = null;

    @Before
    public void setup() {
        testingUncaughtExceptionHandler = new TestingUncaughtExceptionHandler();
        robustActorSystem =
                RobustActorSystem.create(
                        "testSystem",
                        AkkaUtils.getForkJoinExecutorConfig(
                                RpcUtils.getTestForkJoinExecutorConfiguration()),
                        testingUncaughtExceptionHandler);
    }

    @After
    public void teardown() {
        robustActorSystem.terminate();
        testingUncaughtExceptionHandler = null;
    }

    @Test
    public void testUncaughtExceptionHandler() {
        final Error error = new UnknownError("Foobar");

        robustActorSystem
                .dispatcher()
                .execute(
                        () -> {
                            throw error;
                        });

        final Throwable uncaughtException =
                testingUncaughtExceptionHandler.waitForUncaughtException();

        assertThat(uncaughtException, is(error));
    }

    @Test
    public void testUncaughtExceptionHandlerFromActor() {
        final Error error = new UnknownError();
        final ActorRef actor =
                robustActorSystem.actorOf(Props.create(UncaughtExceptionActor.class, error));

        actor.tell(new Failure(), null);

        final Throwable uncaughtException =
                testingUncaughtExceptionHandler.waitForUncaughtException();

        assertThat(uncaughtException, is(error));
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
