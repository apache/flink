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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import org.apache.flink.shaded.guava33.com.google.common.math.LongMath;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A delegator holds user's {@link AsyncLookupFunction} to handle retries. */
public class RetryableAsyncLookupFunctionDelegator extends AsyncLookupFunction {

    private static final long serialVersionUID = 2779862427737869086L;

    private final AsyncLookupFunction userLookupFunction;

    private final ResultRetryStrategy retryStrategy;

    private final boolean retryEnabled;

    private transient Predicate<Collection<RowData>> retryResultPredicate;

    private transient ProcessingTimeService processingTimeService;

    /** Pending retry timers guarded by this instance. Null when retries are stopped. */
    private transient Map<CompletableFuture<Collection<RowData>>, ScheduledFuture<?>> retryTimers;

    public RetryableAsyncLookupFunctionDelegator(
            @Nonnull AsyncLookupFunction userLookupFunction,
            @Nonnull ResultRetryStrategy retryStrategy) {
        this.userLookupFunction = checkNotNull(userLookupFunction);
        this.retryStrategy = checkNotNull(retryStrategy);
        this.retryEnabled = retryStrategy.getRetryPredicate().resultPredicate().isPresent();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        retryResultPredicate =
                retryStrategy.getRetryPredicate().resultPredicate().orElse(ignore -> false);
        if (retryEnabled) {
            checkNotNull(
                    processingTimeService, "The lookup retry needs a processing time service.");
            retryTimers = new HashMap<>();
        }
        userLookupFunction.open(context);
    }

    void setProcessingTimeService(ProcessingTimeService processingTimeService) {
        this.processingTimeService = processingTimeService;
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        if (!retryEnabled) {
            return userLookupFunction.asyncLookup(keyRow);
        }
        CompletableFuture<Collection<RowData>> resultFuture = new CompletableFuture<>();
        lookupWithRetry(resultFuture, 1, keyRow);
        return resultFuture;
    }

    private void lookupWithRetry(
            final CompletableFuture<Collection<RowData>> resultFuture,
            final int currentAttempts,
            final RowData keyRow) {
        CompletableFuture<Collection<RowData>> lookupFuture =
                userLookupFunction.asyncLookup(keyRow);

        lookupFuture.whenCompleteAsync(
                (result, throwable) -> {
                    if (retryResultPredicate.test(result)
                            && retryStrategy.canRetry(currentAttempts)) {
                        scheduleRetry(resultFuture, currentAttempts, keyRow);
                    } else {
                        resultFuture.complete(result);
                    }
                });
    }

    private synchronized void scheduleRetry(
            CompletableFuture<Collection<RowData>> resultFuture,
            int currentAttempts,
            RowData keyRow) {
        if (retryTimers == null) {
            return;
        }
        long retryTimestamp =
                LongMath.saturatedAdd(
                        processingTimeService.getCurrentProcessingTime(),
                        retryStrategy.getBackoffTimeMillis(currentAttempts));
        // The operator's processing time service dispatches timer callbacks to the task mailbox.
        ScheduledFuture<?> timer =
                processingTimeService.registerTimer(
                        retryTimestamp,
                        timestamp -> retryLookup(resultFuture, currentAttempts + 1, keyRow));
        retryTimers.put(resultFuture, timer);
    }

    private void retryLookup(
            CompletableFuture<Collection<RowData>> resultFuture,
            int currentAttempts,
            RowData keyRow) {
        synchronized (this) {
            // A timer callback can already be queued in the mailbox when retries are stopped.
            if (retryTimers == null || retryTimers.remove(resultFuture) == null) {
                return;
            }
        }
        try {
            lookupWithRetry(resultFuture, currentAttempts, keyRow);
        } catch (Throwable ignored) {
            // The previous whenCompleteAsync callback captured synchronous retry failures in its
            // unused completion stage. Leave the result pending for the operator timeout.
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (retryTimers != null) {
                retryTimers.values().forEach(timer -> timer.cancel(false));
                retryTimers = null;
            }
        }
        userLookupFunction.close();
        super.close();
    }

    public AsyncLookupFunction getUserLookupFunction() {
        return userLookupFunction;
    }
}
