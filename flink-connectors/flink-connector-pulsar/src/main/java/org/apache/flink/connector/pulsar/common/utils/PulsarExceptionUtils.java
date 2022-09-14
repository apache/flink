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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Util class for pulsar checked exceptions. Sneaky throw {@link PulsarAdminException} and {@link
 * PulsarClientException}.
 */
@Internal
public final class PulsarExceptionUtils {

    private PulsarExceptionUtils() {
        // No public constructor.
    }

    public static <R extends PulsarClientException> void sneakyClient(
            ThrowingRunnable<R> runnable) {
        sneaky(runnable);
    }

    public static <T, R extends PulsarClientException> T sneakyClient(
            SupplierWithException<T, R> supplier) {
        return sneaky(supplier);
    }

    public static <R extends PulsarAdminException> void sneakyAdmin(ThrowingRunnable<R> runnable) {
        sneaky(runnable);
    }

    public static <T, R extends PulsarAdminException> T sneakyAdmin(
            SupplierWithException<T, R> supplier) {
        return sneaky(supplier);
    }

    private static <R extends Exception> void sneaky(ThrowingRunnable<R> runnable) {
        try {
            runnable.run();
        } catch (Exception r) {
            sneakyThrow(r);
        }
    }

    /** Catch the throwable exception and rethrow it without try catch. */
    private static <T, R extends Exception> T sneaky(SupplierWithException<T, R> supplier) {
        try {
            return supplier.get();
        } catch (Exception r) {
            sneakyThrow(r);
        }

        // This method wouldn't be executed.
        throw new RuntimeException("Never throw here.");
    }

    /** javac hack for unchecking the checked exception. */
    @SuppressWarnings("unchecked")
    public static <T extends Exception> void sneakyThrow(Exception t) throws T {
        throw (T) t;
    }
}
