/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.CompositeAvailabilityProvider;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompositeAvailabilityProvider}. */
class CompositeAvailabilityProviderTest {

    @Test
    void testSingleChildReturnsChildDirectly() {
        TestAvailabilityProvider child = new TestAvailabilityProvider();
        AvailabilityProvider composite =
                CompositeAvailabilityProvider.of(Collections.singletonList(child));

        assertThat(composite).isSameAs(child);
        assertThat(composite.getAvailableFuture()).isSameAs(child.future);
        assertThat(composite.isAvailable()).isFalse();

        child.future.complete(null);
        assertThat(composite.isAvailable()).isTrue();
    }

    @Test
    void testMultipleChildrenWrappedInComposite() {
        TestAvailabilityProvider first = new TestAvailabilityProvider();
        TestAvailabilityProvider second = new TestAvailabilityProvider();
        List<AvailabilityProvider> children = Arrays.asList(first, second);

        AvailabilityProvider composite = CompositeAvailabilityProvider.of(children);

        assertThat(composite).isInstanceOf(CompositeAvailabilityProvider.class);
        assertThat(composite.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailableFutureCompletesOnlyWhenAllChildrenComplete() {
        TestAvailabilityProvider first = new TestAvailabilityProvider();
        TestAvailabilityProvider second = new TestAvailabilityProvider();
        AvailabilityProvider composite =
                CompositeAvailabilityProvider.of(Arrays.asList(first, second));

        CompletableFuture<?> available = composite.getAvailableFuture();
        assertThat(available.isDone()).isFalse();

        first.future.complete(null);
        assertThat(available.isDone())
                .as("composite future should remain incomplete while one child is pending")
                .isFalse();

        second.future.complete(null);
        assertThat(available.isDone()).isTrue();
        assertThat(composite.isAvailable()).isTrue();
    }

    /** Minimal {@link AvailabilityProvider} backed by a mutable {@link CompletableFuture}. */
    private static final class TestAvailabilityProvider implements AvailabilityProvider {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public CompletableFuture<?> getAvailableFuture() {
            return future;
        }
    }
}
