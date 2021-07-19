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

package org.apache.flink.connector.pulsar.source.exception;

/** Util class for pulsar checked exceptions. */
public final class ThrowingExceptionUtils {

    private ThrowingExceptionUtils() {
        // No public constructor.
    }

    /** Catch the throwable exception and wrap it into a {@link PulsarRuntimeException} */
    public static <T, R extends Exception> T checked(ThrowingSupplier<T, R> supplier) {
        try {
            return supplier.get();
        } catch (Exception r) {
            throw new PulsarRuntimeException(r);
        }
    }

    /** Catch the throwable exception and rethrow it without try catch. */
    public static <T, R extends Exception> T sneaky(ThrowingSupplier<T, R> supplier) {
        try {
            return supplier.get();
        } catch (Exception r) {
            sneakyThrow(r);
        }

        // This method wouldn't be executed.
        return null;
    }

    /** javac hack for unchecking the checked exception */
    private static <T extends Exception> void sneakyThrow(Exception t) throws T {
        throw (T) t;
    }
}
