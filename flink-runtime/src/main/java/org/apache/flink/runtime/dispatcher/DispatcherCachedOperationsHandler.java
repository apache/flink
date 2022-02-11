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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.CompletedOperationCache;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A handler for async operations triggered by the {@link Dispatcher} whose keys and results are
 * cached.
 */
public class DispatcherCachedOperationsHandler {

    private final CompletedOperationCache<AsynchronousJobOperationKey, String>
            savepointTriggerCache;

    private final TriggerSavepointFunction triggerSavepointFunction;

    private final TriggerSavepointFunction stopWithSavepointFunction;

    DispatcherCachedOperationsHandler(
            DispatcherOperationCaches operationCaches,
            TriggerSavepointFunction triggerSavepointFunction,
            TriggerSavepointFunction stopWithSavepointFunction) {
        this(
                triggerSavepointFunction,
                stopWithSavepointFunction,
                operationCaches.getSavepointTriggerCache());
    }

    @VisibleForTesting
    DispatcherCachedOperationsHandler(
            TriggerSavepointFunction triggerSavepointFunction,
            TriggerSavepointFunction stopWithSavepointFunction,
            CompletedOperationCache<AsynchronousJobOperationKey, String> savepointTriggerCache) {
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.stopWithSavepointFunction = stopWithSavepointFunction;
        this.savepointTriggerCache = savepointTriggerCache;
    }

    public CompletableFuture<Acknowledge> triggerSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return registerOperationIdempotently(
                operationKey,
                () ->
                        triggerSavepointFunction.apply(
                                operationKey.getJobId(),
                                targetDirectory,
                                formatType,
                                savepointMode,
                                timeout));
    }

    public CompletableFuture<Acknowledge> stopWithSavepoint(
            AsynchronousJobOperationKey operationKey,
            String targetDirectory,
            SavepointFormatType formatType,
            TriggerSavepointMode savepointMode,
            Time timeout) {
        return registerOperationIdempotently(
                operationKey,
                () ->
                        stopWithSavepointFunction.apply(
                                operationKey.getJobId(),
                                targetDirectory,
                                formatType,
                                savepointMode,
                                timeout));
    }

    public CompletableFuture<OperationResult<String>> getSavepointStatus(
            AsynchronousJobOperationKey operationKey) {
        return savepointTriggerCache
                .get(operationKey)
                .map(CompletableFuture::completedFuture)
                .orElse(
                        FutureUtils.completedExceptionally(
                                new UnknownOperationKeyException(operationKey)));
    }

    private CompletableFuture<Acknowledge> registerOperationIdempotently(
            AsynchronousJobOperationKey operationKey,
            Supplier<CompletableFuture<String>> operation) {
        if (!savepointTriggerCache.containsOperation(operationKey)) {
            savepointTriggerCache.registerOngoingOperation(operationKey, operation.get());
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }
}
