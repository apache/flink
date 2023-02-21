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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * {@code StateHandleStoreUtilsTest} tests the utility classes collected in {@link
 * StateHandleStoreUtils}.
 */
public class StateHandleStoreUtilsTest extends TestLogger {

    @Test
    public void testSerializationAndDeserialization() throws Exception {
        final TestingLongStateHandleHelper.LongStateHandle original =
                new TestingLongStateHandleHelper.LongStateHandle(42L);
        byte[] serializedData = StateHandleStoreUtils.serializeOrDiscard(original);

        final TestingLongStateHandleHelper.LongStateHandle deserializedInstance =
                StateHandleStoreUtils.deserialize(serializedData);
        assertThat(deserializedInstance.getStateSize(), is(original.getStateSize()));
        assertThat(deserializedInstance.getValue(), is(original.getValue()));
    }

    @Test
    public void testSerializeOrDiscardFailureHandling() throws Exception {
        final AtomicBoolean discardCalled = new AtomicBoolean(false);
        final StateObject original =
                new FailingSerializationStateObject(() -> discardCalled.set(true));

        try {
            StateHandleStoreUtils.serializeOrDiscard(original);
            fail("An IOException is expected to be thrown.");
        } catch (IOException e) {
            // IOException is expected
        }

        assertThat(discardCalled.get(), is(true));
    }

    @Test
    public void testSerializationOrDiscardWithDiscardFailure() throws Exception {
        final Exception discardException =
                new IllegalStateException(
                        "Expected IllegalStateException that should be suppressed.");
        final StateObject original =
                new FailingSerializationStateObject(
                        () -> {
                            throw discardException;
                        });

        try {
            StateHandleStoreUtils.serializeOrDiscard(original);
            fail("An IOException is expected to be thrown.");
        } catch (IOException e) {
            // IOException is expected
            assertThat(e.getSuppressed().length, is(1));
            assertThat(e.getSuppressed()[0], is(discardException));
        }
    }

    private static class FailingSerializationStateObject implements StateObject {

        private static final long serialVersionUID = 6382458109061973983L;
        private final RunnableWithException discardStateRunnable;

        public FailingSerializationStateObject(RunnableWithException discardStateRunnable) {
            this.discardStateRunnable = discardStateRunnable;
        }

        private void writeObject(ObjectOutputStream outputStream) throws IOException {
            throw new IOException("Expected IOException to test serialization error.");
        }

        @Override
        public void discardState() throws Exception {
            discardStateRunnable.run();
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }
}
