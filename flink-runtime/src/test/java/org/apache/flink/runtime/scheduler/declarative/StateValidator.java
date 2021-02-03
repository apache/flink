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

import org.apache.flink.util.Preconditions;

import java.util.function.Consumer;

/**
 * Utility for state test classes (e.g. {@link WaitingForResourcesTest}) to track if correct input
 * has been presented and if the state transition happened.
 *
 * @param <T> Type of the state to validate.
 */
public class StateValidator<T> {

    private Runnable trap = () -> {};
    private Consumer<T> consumer = null;
    private final String stateName;

    public StateValidator(String stateName) {
        this.stateName = stateName;
    }

    /**
     * Activate this validator (if the state transition hasn't been validated, it will fail in the
     * close method).
     *
     * @param asserter Consumer which validates the input to the state transition.
     */
    public void activate(Consumer<T> asserter) {
        consumer = Preconditions.checkNotNull(asserter);
        trap =
                () -> {
                    throw new AssertionError("no transition to " + stateName);
                };
    }

    /**
     * Call this method on the state transition, to register the transition, and validate the passed
     * arguments.
     *
     * @param input Argument(s) of the state transition.
     * @throws NullPointerException If no comsumer has been set (an unexpected state transition
     *     occurred)
     */
    public void validateInput(T input) {
        Preconditions.checkNotNull(consumer, "No consumer set. Unexpected state transition?");
        trap = () -> {};
        consumer.accept(input);
    }

    /**
     * If the validator has been activated, check if input has been provided (e.g. a state
     * transition happened).
     */
    public void close() {
        trap.run();
    }
}
