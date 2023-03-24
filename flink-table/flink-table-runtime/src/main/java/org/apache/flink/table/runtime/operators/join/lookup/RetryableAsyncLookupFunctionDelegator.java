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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A delegator holds user's {@link AsyncLookupFunction} to handle retries. */
public class RetryableAsyncLookupFunctionDelegator extends AsyncLookupFunction {

    private final AsyncLookupFunction userLookupFunction;

    private final ResultRetryStrategy retryStrategy;

    private final boolean retryEnabled;

    private transient Predicate<Collection<RowData>> retryResultPredicate;

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
        userLookupFunction.open(context);
        retryResultPredicate =
                retryStrategy.getRetryPredicate().resultPredicate().orElse(ignore -> false);
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
                        long backoff = retryStrategy.getBackoffTimeMillis(currentAttempts);
                        try {
                            Thread.sleep(backoff);
                        } catch (InterruptedException e) {
                            // Do not raise an error when interrupted, just complete with last
                            // result intermediately.
                            resultFuture.complete(result);
                            return;
                        }
                        lookupWithRetry(resultFuture, currentAttempts + 1, keyRow);
                    } else {
                        resultFuture.complete(result);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        userLookupFunction.close();
        super.close();
    }
}
