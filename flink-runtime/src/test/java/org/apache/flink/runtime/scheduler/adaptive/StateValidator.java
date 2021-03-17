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

import org.apache.flink.util.Preconditions;

import java.util.function.Consumer;

import static org.junit.Assert.fail;

/**
 * Utility for state test classes (e.g. {@link WaitingForResourcesTest}) to track if correct input
 * has been presented and if the state transition happened.
 *
 * @param <T> Type of the state to validate.
 */
public class StateValidator<T> {

    private Runnable trap = () -> {};
    private Consumer<T> consumer;
    private final String stateName;

    public StateValidator(String stateName) {
        this.stateName = stateName;
        expectNoStateTransition();
    }

    /**
     * Expect an input, and validate it with the given asserter (if the state transition hasn't been
     * validated, it will fail in the close method).
     *
     * @param asserter Consumer which validates the input to the state transition.
     */
    public void expectInput(Consumer<T> asserter) {
        consumer = Preconditions.checkNotNull(asserter);
        trap =
                () -> {
                    throw new AssertionError("No transition to " + stateName);
                };
    }

    /**
     * Call this method on the state transition, to register the transition, and validate the passed
     * arguments.
     *
     * @param input Argument(s) of the state transition.
     * @throws NullPointerException If no consumer has been set (an unexpected state transition
     *     occurred)
     */
    public void validateInput(T input) {
        trap = () -> {};
        consumer.accept(input);
        expectNoStateTransition();
    }

    /**
     * If the validator has been activated, check if input has been provided (e.g. a state
     * transition happened).
     */
    public void close() {
        trap.run();
    }

    public final void expectNoStateTransition() {
        consumer =
                (T) ->
                        fail(
                                "No consumer has been set for "
                                        + stateName
                                        + ". Unexpected state transition");
    }
}
