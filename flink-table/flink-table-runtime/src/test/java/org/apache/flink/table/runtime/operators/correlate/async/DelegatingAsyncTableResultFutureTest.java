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

package org.apache.flink.table.runtime.operators.correlate.async;

import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import org.junit.jupiter.api.Test;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link DelegatingAsyncTableResultFuture}. */
class DelegatingAsyncTableResultFutureTest {

    @Test
    void wrapExceptionIsForwardedToDelegatedResultFuture() {
        // needsWrapping=true + isInternalResultType=false → wrapExternal, which calls Row.of on
        // every element. The non-public, non-static `wrapExternal` does NOT itself throw on
        // null elements (Row.of allows them), so we drive the wrap failure by completing the
        // future with a Collection whose iteration throws. This proves the new try/catch in
        // accept() forwards the failure as completeExceptionally rather than letting it
        // propagate on the completing thread and stranding the ResultFuture.
        CapturingResultFuture downstream = new CapturingResultFuture();
        DelegatingAsyncTableResultFuture bridge =
                new DelegatingAsyncTableResultFuture(
                        downstream, /* needsWrapping */ true, /* isInternalResultType */ false);

        RuntimeException boom = new RuntimeException("wrap boom");
        bridge.getCompletableFuture().complete(new ThrowingOnIterateCollection(boom));

        assertThat(downstream.error.get()).isSameAs(boom);
        assertThat(downstream.completed.get()).isNull();
    }

    @Test
    void exceptionalCompletionShortCircuitsWrap() {
        // Sanity case: the throwable branch must NOT touch wrapFunction.
        CapturingResultFuture downstream = new CapturingResultFuture();
        DelegatingAsyncTableResultFuture bridge =
                new DelegatingAsyncTableResultFuture(
                        downstream, /* needsWrapping */ true, /* isInternalResultType */ false);

        RuntimeException boom = new RuntimeException("user boom");
        bridge.getCompletableFuture().completeExceptionally(boom);

        assertThat(downstream.error.get()).isSameAs(boom);
        assertThat(downstream.completed.get()).isNull();
    }

    @Test
    void identityWrappingForwardsCollectionContents() {
        // needsWrapping=false → wrapFunction is identity; the delegate must observe the same
        // payload without going through the exceptional path.
        CapturingResultFuture downstream = new CapturingResultFuture();
        DelegatingAsyncTableResultFuture bridge =
                new DelegatingAsyncTableResultFuture(
                        downstream, /* needsWrapping */ false, /* isInternalResultType */ false);

        Collection<Object> outs = Collections.singletonList("payload");
        bridge.getCompletableFuture().complete(outs);

        assertThat(downstream.completed.get()).containsExactly("payload");
        assertThat(downstream.error.get()).isNull();
    }

    /**
     * Collection whose iterator throws on first call — used to force wrapExternal / wrapInternal to
     * fail during {@code wrapFunction.apply(outs)} inside accept().
     */
    private static final class ThrowingOnIterateCollection extends AbstractCollection<Object> {
        private final RuntimeException toThrow;

        ThrowingOnIterateCollection(RuntimeException toThrow) {
            this.toThrow = toThrow;
        }

        @Override
        public Iterator<Object> iterator() {
            throw toThrow;
        }

        @Override
        public int size() {
            return 1;
        }
    }

    /** Minimal ResultFuture that records the last complete/completeExceptionally call. */
    private static final class CapturingResultFuture implements ResultFuture<Object> {
        final AtomicReference<Collection<Object>> completed = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override
        public void complete(Collection<Object> result) {
            completed.set(result);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            this.error.set(error);
        }

        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            throw new UnsupportedOperationException();
        }
    }
}
