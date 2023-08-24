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

package org.apache.flink.runtime.scheduler.adaptive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for the default methods on the {@link State} interface, based on the {@link Created} state,
 * as it is a simple state.
 */
class StateTest {

    private static final Logger LOG = LoggerFactory.getLogger(StateTest.class);

    @RegisterExtension CreatedTest.MockCreatedContext ctx = new CreatedTest.MockCreatedContext();

    @Test
    void testEmptyAs() {
        State state = new Created(ctx, LOG);
        assertThat(state.as(WaitingForResources.class)).isEmpty();
    }

    @Test
    void testCast() {
        Created state = new Created(ctx, LOG);
        assertThat(state.as(Created.class)).hasValue(state);
    }

    @Test
    void testTryRunStateMismatch() {
        State state = new Created(ctx, LOG);
        state.tryRun(WaitingForResources.class, waiting -> fail("Unexpected execution"), "test");
    }

    @Test
    void testTryRun() {
        State state = new Created(ctx, LOG);
        AtomicBoolean called = new AtomicBoolean(false);
        state.tryRun(Created.class, created -> called.set(true), "test");
        assertThat(called).isTrue();
    }

    @Test
    void testTryCallStateMismatch() {
        State state = new Created(ctx, LOG);
        Optional<String> result =
                state.tryCall(
                        WaitingForResources.class,
                        Waiting -> {
                            fail("Unexpected execution");
                            return "nope";
                        },
                        "test");
        assertThat(result).isEmpty();
    }

    @Test
    void testTryCall() {
        State state = new Created(ctx, LOG);
        Optional<String> result = state.tryCall(Created.class, created -> "yes", "test");
        assertThat(result).hasValue("yes");
    }
}
