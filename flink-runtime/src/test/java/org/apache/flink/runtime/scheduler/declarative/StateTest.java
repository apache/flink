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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the default methods on the {@link State} interface, based on the {@link Created} state,
 * as it is a simple state.
 */
public class StateTest extends TestLogger {
    @Test
    public void testEmptyAs() throws Exception {
        try (CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext()) {
            State state = new Created(ctx, log);
            assertThat(state.as(WaitingForResources.class), is(Optional.empty()));
        }
    }

    @Test
    public void testCast() throws Exception {
        try (CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext()) {
            State state = new Created(ctx, log);
            assertThat(state.as(Created.class), is(Optional.of(state)));
        }
    }

    @Test
    public void testTryRunNoRun() throws Exception {
        try (CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext()) {
            State state = new Created(ctx, log);
            state.tryRun(
                    WaitingForResources.class, (waiting -> fail("Unexpected execution")), "test");
        }
    }

    @Test
    public void testTryRun() throws Exception {
        try (CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext()) {
            State state = new Created(ctx, log);
            Tuple1<Runnable> validate = Tuple1.of(() -> fail("Did not run"));
            state.tryRun(Created.class, created -> validate.setFields(() -> {}), "test");
            validate.f0.run();
        }
    }

    @Test
    public void testTryCallNoCall() throws Exception {
        try (CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext()) {
            State state = new Created(ctx, log);
            Optional<String> result =
                    state.tryCall(
                            WaitingForResources.class,
                            Waiting -> {
                                fail("Unexpected execution");
                                return "nope";
                            },
                            "test");
            assertThat(result, is(Optional.empty()));
        }
    }

    @Test
    public void testTryCall() throws Exception {
        try (CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext()) {
            State state = new Created(ctx, log);
            Tuple1<Runnable> validate = Tuple1.of(() -> fail("Did not run"));
            Optional<String> result =
                    state.tryCall(
                            Created.class,
                            created -> {
                                validate.setFields(() -> {});
                                return "yes";
                            },
                            "test");
            validate.f0.run();
            assertThat(result, is(Optional.of("yes")));
        }
    }
}
