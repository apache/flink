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

/** Testing {@link TimerService} implementation. */
public class TestingTimerService<T> implements TimerService<T> {
    private final Consumer<TimeoutListener<T>> startConsumer;
    private final Runnable stopConsumer;
    private final TriConsumer<T, Long, TimeUnit> registerTimeoutConsumer;
    private final Consumer<T> unregisterTimeoutConsumer;
    private final BiFunction<T, UUID, Boolean> isValidFunction;

    public TestingTimerService(
            Consumer<TimeoutListener<T>> startConsumer,
            Runnable stopConsumer,
            TriConsumer<T, Long, TimeUnit> registerTimeoutConsumer,
            Consumer<T> unregisterTimeoutConsumer,
            BiFunction<T, UUID, Boolean> isValidFunction) {
        this.startConsumer = startConsumer;
        this.stopConsumer = stopConsumer;
        this.registerTimeoutConsumer = registerTimeoutConsumer;
        this.unregisterTimeoutConsumer = unregisterTimeoutConsumer;
        this.isValidFunction = isValidFunction;
    }

    @Override
    public void start(TimeoutListener<T> timeoutListener) {
        startConsumer.accept(timeoutListener);
    }

    @Override
    public void stop() {
        stopConsumer.run();
    }

    @Override
    public void registerTimeout(T key, long delay, TimeUnit unit) {
        registerTimeoutConsumer.accept(key, delay, unit);
    }

    @Override
    public void unregisterTimeout(T key) {
        unregisterTimeoutConsumer.accept(key);
    }

    @Override
    public boolean isValid(T key, UUID ticket) {
        return isValidFunction.apply(key, ticket);
    }
}
