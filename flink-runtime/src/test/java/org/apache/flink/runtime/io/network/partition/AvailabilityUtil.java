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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Assertions for availability futures. */
public class AvailabilityUtil {

    public static <E extends Throwable> void assertFutureCompletion(
            boolean after,
            Supplier<CompletableFuture<?>> futureSupplier,
            boolean before,
            ThrowingRunnable<E> runnable)
            throws E {
        final CompletableFuture<?> availableFuture = futureSupplier.get();
        assertEquals(before, availableFuture.isDone());
        runnable.run();
        assertEquals(after, futureSupplier.get().isDone());
        if (after) {
            assertTrue(availableFuture.isDone());
        }
    }

    public static <E extends Throwable> void assertAvailability(
            AvailabilityProvider availabilityProvider,
            boolean before,
            boolean after,
            ThrowingRunnable<E> runnable)
            throws E {
        assertFutureCompletion(after, availabilityProvider::getAvailableFuture, before, runnable);
    }

    public static <E extends Throwable> void assertPriorityAvailability(
            InputGate inputGate, boolean before, boolean after, ThrowingRunnable<E> runnable)
            throws E {
        assertFutureCompletion(after, inputGate::getPriorityEventAvailableFuture, before, runnable);
    }
}
