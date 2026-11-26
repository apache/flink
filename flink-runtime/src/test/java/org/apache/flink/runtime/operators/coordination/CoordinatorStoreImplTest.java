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

package org.apache.flink.runtime.operators.coordination;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link CoordinatorStoreImpl}. */
class CoordinatorStoreImplTest {

    @Test
    void testApplyOnAbsentKeyDoesNotInsertEntry() {
        CoordinatorStoreImpl store = new CoordinatorStoreImpl();

        Object result = store.apply("missing", value -> value == null ? "absent" : "present");

        assertThat(result).isEqualTo("absent");
        assertThat(store.containsKey("missing")).isFalse();
    }

    @Test
    void testApplyIsAtomicWithComputeIfPresent() throws Exception {
        CoordinatorStoreImpl store = new CoordinatorStoreImpl();
        StringBuilder mutableValue = new StringBuilder("a");
        store.putIfAbsent("k", mutableValue);

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch readerDone = new CountDownLatch(1);

        Thread reader =
                new Thread(
                        () -> {
                            readerStarted.countDown();
                            try {
                                while (!stop.get() && failure.get() == null) {
                                    int length =
                                            store.apply("k", v -> ((StringBuilder) v).length());
                                    if (length != 1) {
                                        failure.compareAndSet(
                                                null,
                                                new AssertionError(
                                                        "Observed torn intermediate length: "
                                                                + length));
                                        return;
                                    }
                                }
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            } finally {
                                readerDone.countDown();
                            }
                        });
        reader.start();
        readerStarted.await();

        try {
            for (int i = 0; i < 50_000 && failure.get() == null; i++) {
                store.computeIfPresent(
                        "k",
                        (k, v) -> {
                            StringBuilder sb = (StringBuilder) v;
                            sb.setLength(0);
                            sb.append('a');
                            return sb;
                        });
            }
        } finally {
            stop.set(true);
            readerDone.await();
            reader.join();
        }

        assertThat(failure.get()).isNull();
    }
}
