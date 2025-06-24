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

package org.apache.flink.runtime.rest.handler.job.savepoints;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.util.function.TriFunction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Utility functions used in tests. */
final class SavepointTestUtilities {

    /**
     * Returns a function which, when called, sets the provided reference to the {@link
     * AsynchronousJobOperationKey} that the function was called with, then returns a future
     * containing an {@link Acknowledge}.
     *
     * @param keyReference the reference to set to the operation key
     * @return function which sets the reference once called
     */
    public static TriFunction<
                    AsynchronousJobOperationKey,
                    String,
                    SavepointFormatType,
                    CompletableFuture<Acknowledge>>
            setReferenceToOperationKey(AtomicReference<AsynchronousJobOperationKey> keyReference) {
        return (AsynchronousJobOperationKey operationKey,
                String directory,
                SavepointFormatType formatType) -> {
            keyReference.set(operationKey);
            return CompletableFuture.completedFuture(Acknowledge.get());
        };
    }

    /**
     * Returns a function which returns the provided result when called with the key that the
     * provided reference points to. If called with any other key, it throws a {@link
     * RuntimeException}.
     *
     * @param resultToReturn the result to return if called with the correct key
     * @param expectedKeyReference reference to the expected key
     * @return function which returns the result if called with the specified key
     */
    public static Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
            getResultIfKeyMatches(
                    OperationResult<String> resultToReturn,
                    AtomicReference<AsynchronousJobOperationKey> expectedKeyReference) {
        return (AsynchronousJobOperationKey operationKey) -> {
            if (operationKey.equals(expectedKeyReference.get())) {
                return CompletableFuture.completedFuture(resultToReturn);
            }
            throw new RuntimeException(
                    "Expected operation key "
                            + expectedKeyReference.get()
                            + ", but received "
                            + operationKey);
        };
    }

    private SavepointTestUtilities() {}
}
