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

package org.apache.flink.runtime.state;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.flink.runtime.concurrent.FutureUtils.completedExceptionally;
import static org.apache.flink.runtime.state.StateUtil.discardStateFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link StateUtil}. */
public class StateUtilTest {

    @Test
    public void testDiscardStateSize() throws Exception {
        assertEquals(1234, discardStateFuture(completedFuture(new TestStateObject(1234))));
        assertEquals(0, discardStateFuture(null));
        assertEquals(0, discardStateFuture(new CompletableFuture<>()));
        assertEquals(0, discardStateFuture(completedExceptionally(new RuntimeException())));
        assertEquals(0, discardStateFuture(emptyFuture(false, true)));
        assertEquals(0, discardStateFuture(emptyFuture(false, false)));
        assertEquals(0, discardStateFuture(emptyFuture(true, true)));
        assertEquals(0, discardStateFuture(emptyFuture(true, false)));
    }

    @Test
    public void unexpectedStateExceptionForSingleExpectedType() {
        Exception exception =
                StateUtil.unexpectedStateHandleException(
                        KeyGroupsStateHandle.class, KeyGroupsStateHandle.class);

        assertThat(
                exception.getMessage(),
                containsString(
                        "Unexpected state handle type, expected one of: class org.apache.flink.runtime.state.KeyGroupsStateHandle, but found: class org.apache.flink.runtime.state.KeyGroupsStateHandle. This can mostly happen when a different StateBackend from the one that was used for taking a checkpoint/savepoint is used when restoring."));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unexpectedStateExceptionForMultipleExpectedTypes() {
        Exception exception =
                StateUtil.unexpectedStateHandleException(
                        new Class[] {KeyGroupsStateHandle.class, KeyGroupsStateHandle.class},
                        KeyGroupsStateHandle.class);

        assertThat(
                exception.getMessage(),
                containsString(
                        "Unexpected state handle type, expected one of: class org.apache.flink.runtime.state.KeyGroupsStateHandle, class org.apache.flink.runtime.state.KeyGroupsStateHandle, but found: class org.apache.flink.runtime.state.KeyGroupsStateHandle. This can mostly happen when a different StateBackend from the one that was used for taking a checkpoint/savepoint is used when restoring."));
    }

    private static <T> Future<T> emptyFuture(boolean done, boolean canBeCancelled) {
        return new Future<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return canBeCancelled;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return done;
            }

            @Override
            public T get() {
                throw new UnsupportedOperationException();
            }

            @Override
            public T get(long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class TestStateObject implements StateObject {
        private static final long serialVersionUID = -8070326169926626355L;
        private final int size;

        private TestStateObject(int size) {
            this.size = size;
        }

        @Override
        public long getStateSize() {
            return size;
        }

        @Override
        public void discardState() {}
    }
}
