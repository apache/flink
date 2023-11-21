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

package org.apache.flink.streaming.api.operators;

import org.assertj.core.api.Assertions;

import java.util.function.Consumer;

/**
 * TestTrigger is a Triggerable for test. The test trigger take an event time handler or processing
 * time handler, which will be invoked when the trigger is triggered on event time or processing
 * time.
 */
public class TestTrigger<K, N> implements Triggerable<K, N> {

    private final Consumer<InternalTimer<K, N>> eventTimeHandler;
    private final Consumer<InternalTimer<K, N>> processingTimeHandler;

    public static <K, N> TestTrigger<K, N> eventTimeTrigger(
            Consumer<InternalTimer<K, N>> eventTimeHandler) {
        return new TestTrigger<>(
                eventTimeHandler,
                timer -> Assertions.fail("We did not expect processing timer to be triggered."));
    }

    public static <K, N> TestTrigger<K, N> processingTimeTrigger(
            Consumer<InternalTimer<K, N>> processingTimeHandler) {
        return new TestTrigger<>(
                timer -> Assertions.fail("We did not expect event timer to be triggered."),
                processingTimeHandler);
    }

    private TestTrigger(
            Consumer<InternalTimer<K, N>> eventTimeHandler,
            Consumer<InternalTimer<K, N>> processingTimeHandler) {
        this.eventTimeHandler = eventTimeHandler;
        this.processingTimeHandler = processingTimeHandler;
    }

    @Override
    public void onEventTime(InternalTimer<K, N> timer) throws Exception {
        this.eventTimeHandler.accept(timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<K, N> timer) throws Exception {
        this.processingTimeHandler.accept(timer);
    }
}
