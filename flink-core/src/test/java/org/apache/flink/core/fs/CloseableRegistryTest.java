/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.util.AbstractAutoCloseableRegistry;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Tests for the {@link CloseableRegistry}. */
public class CloseableRegistryTest
        extends AbstractAutoCloseableRegistryTest<Closeable, Closeable, Object> {

    @Override
    protected void registerCloseable(final Closeable closeable) throws IOException {
        closeableRegistry.registerCloseable(closeable);
    }

    @Override
    protected AbstractAutoCloseableRegistry<Closeable, Closeable, Object, IOException>
            createRegistry() {

        return new CloseableRegistry();
    }

    @Override
    protected ProducerThread<Closeable, Closeable, Object> createProducerThread(
            AbstractAutoCloseableRegistry<Closeable, Closeable, Object, IOException> registry,
            AtomicInteger unclosedCounter,
            int maxStreams) {

        return new ProducerThread<Closeable, Closeable, Object>(
                registry, unclosedCounter, maxStreams) {
            @Override
            protected void createAndRegisterStream() throws IOException {
                TestStream testStream = new TestStream(unclosedCounter);
                registry.registerCloseable(testStream);
            }
        };
    }

    @Test
    public void testUnregisterAndCloseAll() throws IOException {
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {

            int exTestSize = 5;
            int nonExTestSize = 5;

            List<TestClosable> registeredClosableList = new ArrayList<>(exTestSize + nonExTestSize);
            for (int i = 0; i < nonExTestSize; ++i) {
                registeredClosableList.add(new TestClosable());
            }

            unregisterAndCloseAllHelper(registeredClosableList, closeableRegistry, null);

            for (int i = 0; i < exTestSize; ++i) {
                // Register with exception messages from 1..6
                registeredClosableList.add(new TestClosable(String.valueOf(1 + i)));
            }

            unregisterAndCloseAllHelper(
                    registeredClosableList,
                    closeableRegistry,
                    ioex -> {
                        // Check that error messages and suppressed exceptions are correctly
                        // reported
                        int checksum = 0;
                        checksum += Integer.parseInt(ioex.getMessage());
                        Throwable[] suppressed = ioex.getSuppressed();
                        for (Throwable throwable : suppressed) {
                            checksum += Integer.parseInt(throwable.getMessage());
                        }
                        // Checksum is sum from 1..6 = 15
                        Assert.assertEquals(15, checksum);
                    });

            // Check that unregistered Closable isn't closed.
            TestClosable unregisteredClosable = new TestClosable();
            closeableRegistry.unregisterAndCloseAll(unregisteredClosable);
            Assert.assertEquals(0, unregisteredClosable.getCallsToClose());
        }
    }

    private void unregisterAndCloseAllHelper(
            List<TestClosable> registeredClosableList,
            CloseableRegistry closeableRegistry,
            @Nullable Consumer<IOException> exceptionCheck)
            throws IOException {
        for (TestClosable testClosable : registeredClosableList) {
            closeableRegistry.registerCloseable(testClosable);
        }

        try {
            closeableRegistry.unregisterAndCloseAll(
                    registeredClosableList.toArray(new Closeable[0]));
            if (exceptionCheck != null) {
                Assert.fail("Exception expected");
            }
        } catch (IOException expected) {
            if (exceptionCheck != null) {
                exceptionCheck.accept(expected);
            }
        }

        for (TestClosable testClosable : registeredClosableList) {
            Assert.assertEquals(1, testClosable.getCallsToClose());
            testClosable.resetCallsToClose();
        }
    }

    static class TestClosable implements Closeable {

        private final AtomicInteger callsToClose;
        private final String exceptionMessageOnClose;

        TestClosable() {
            this("");
        }

        TestClosable(String exceptionMessageOnClose) {
            this.exceptionMessageOnClose = exceptionMessageOnClose;
            this.callsToClose = new AtomicInteger(0);
        }

        @Override
        public void close() throws IOException {
            callsToClose.incrementAndGet();
            if (exceptionMessageOnClose != null && exceptionMessageOnClose.length() > 0) {
                throw new IOException(exceptionMessageOnClose);
            }
        }

        public int getCallsToClose() {
            return callsToClose.get();
        }

        public void resetCallsToClose() {
            this.callsToClose.set(0);
        }
    }
}
