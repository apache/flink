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

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.flink.runtime.state.StateUtil.discardStateFuture;
import static org.apache.flink.util.concurrent.FutureUtils.completedExceptionally;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StateUtil}. */
class StateUtilTest {

    @Test
    void testDiscardStateSize() throws Exception {
        assertThat(discardStateFuture(completedFuture(new TestStateObject(1234, 123))))
                .isEqualTo(Tuple2.of(1234L, 123L));
        Tuple2<Long, Long> zeroSize = Tuple2.of(0L, 0L);
        assertThat(discardStateFuture(null)).isEqualTo(zeroSize);
        assertThat(discardStateFuture(new CompletableFuture<>())).isEqualTo(zeroSize);
        assertThat(discardStateFuture(completedExceptionally(new RuntimeException())))
                .isEqualTo(zeroSize);
        assertThat(discardStateFuture(emptyFuture(false, true))).isEqualTo(zeroSize);
        assertThat(discardStateFuture(emptyFuture(false, false))).isEqualTo(zeroSize);
        assertThat(discardStateFuture(emptyFuture(true, true))).isEqualTo(zeroSize);
        assertThat(discardStateFuture(emptyFuture(true, false))).isEqualTo(zeroSize);
    }

    @Test
    void unexpectedStateExceptionForSingleExpectedType() {
        Exception exception =
                StateUtil.unexpectedStateHandleException(
                        KeyGroupsStateHandle.class, KeyGroupsStateHandle.class);

        assertThat(exception.getMessage())
                .contains(
                        "Unexpected state handle type, expected one of: class org.apache.flink.runtime.state.KeyGroupsStateHandle, but found: class org.apache.flink.runtime.state.KeyGroupsStateHandle. This can mostly happen when a different StateBackend from the one that was used for taking a checkpoint/savepoint is used when restoring.");
    }

    @Test
    @SuppressWarnings("unchecked")
    void unexpectedStateExceptionForMultipleExpectedTypes() {
        Exception exception =
                StateUtil.unexpectedStateHandleException(
                        new Class[] {KeyGroupsStateHandle.class, KeyGroupsStateHandle.class},
                        KeyGroupsStateHandle.class);

        assertThat(exception.getMessage())
                .contains(
                        "Unexpected state handle type, expected one of: class org.apache.flink.runtime.state.KeyGroupsStateHandle, class org.apache.flink.runtime.state.KeyGroupsStateHandle, but found: class org.apache.flink.runtime.state.KeyGroupsStateHandle. This can mostly happen when a different StateBackend from the one that was used for taking a checkpoint/savepoint is used when restoring.");
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

    private static class TestStateObject implements CompositeStateHandle {
        private static final long serialVersionUID = -8070326169926626355L;
        private final int size;
        private final int checkpointedSize;

        private TestStateObject(int size, int checkpointedSize) {
            this.size = size;
            this.checkpointedSize = checkpointedSize;
        }

        @Override
        public long getStateSize() {
            return size;
        }

        @Override
        public void discardState() {}

        @Override
        public long getCheckpointedSize() {
            return checkpointedSize;
        }

        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
            throw new UnsupportedOperationException();
        }
    }
}
