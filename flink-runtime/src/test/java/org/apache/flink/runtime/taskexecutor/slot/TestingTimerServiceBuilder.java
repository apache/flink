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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.util.function.TriConsumer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/** Builder for {@link TestingTimerService}. */
public class TestingTimerServiceBuilder<T> {
    private Consumer<TimeoutListener<T>> startConsumer = ignored -> {};
    private Runnable stopConsumer = () -> {};
    private TriConsumer<T, Long, TimeUnit> registerTimeoutConsumer =
            (ignoredA, ignoredB, ignoredC) -> {};
    private Consumer<T> unregisterTimeoutConsumer = ignored -> {};
    private BiFunction<T, UUID, Boolean> isValidFunction = (ignoredA, ignoredB) -> true;

    public TestingTimerServiceBuilder<T> setStartConsumer(
            Consumer<TimeoutListener<T>> startConsumer) {
        this.startConsumer = startConsumer;
        return this;
    }

    public TestingTimerServiceBuilder<T> setStopConsumer(Runnable stopConsumer) {
        this.stopConsumer = stopConsumer;
        return this;
    }

    public TestingTimerServiceBuilder<T> setRegisterTimeoutConsumer(
            TriConsumer<T, Long, TimeUnit> registerTimeoutConsumer) {
        this.registerTimeoutConsumer = registerTimeoutConsumer;
        return this;
    }

    public TestingTimerServiceBuilder<T> setUnregisterTimeoutConsumer(
            Consumer<T> unregisterTimeoutConsumer) {
        this.unregisterTimeoutConsumer = unregisterTimeoutConsumer;
        return this;
    }

    public TestingTimerServiceBuilder<T> setIsValidFunction(
            BiFunction<T, UUID, Boolean> isValidFunction) {
        this.isValidFunction = isValidFunction;
        return this;
    }

    public TestingTimerService<T> createTestingTimerService() {
        return new TestingTimerService<T>(
                startConsumer,
                stopConsumer,
                registerTimeoutConsumer,
                unregisterTimeoutConsumer,
                isValidFunction);
    }
}
