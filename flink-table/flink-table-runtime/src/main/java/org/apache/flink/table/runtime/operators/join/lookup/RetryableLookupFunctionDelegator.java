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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Predicate;

/** A delegator holds user's {@link LookupFunction} to handle retries. */
public class RetryableLookupFunctionDelegator extends LookupFunction {

    private final LookupFunction userLookupFunction;

    private final ResultRetryStrategy retryStrategy;

    private final boolean retryEnabled;

    private transient Predicate<Collection<RowData>> retryResultPredicate;

    public RetryableLookupFunctionDelegator(
            @Nonnull LookupFunction userLookupFunction,
            @Nonnull ResultRetryStrategy retryStrategy) {
        this.userLookupFunction = userLookupFunction;
        this.retryStrategy = retryStrategy;
        this.retryEnabled = retryStrategy.getRetryPredicate().resultPredicate().isPresent();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        if (!retryEnabled) {
            return userLookupFunction.lookup(keyRow);
        }
        for (int attemptNumber = 1; ; attemptNumber++) {
            Collection<RowData> result = userLookupFunction.lookup(keyRow);
            if (retryResultPredicate.test(result) && retryStrategy.canRetry(attemptNumber)) {
                long backoff = retryStrategy.getBackoffTimeMillis(attemptNumber);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return result;
            }
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        userLookupFunction.open(context);
        retryResultPredicate =
                retryStrategy.getRetryPredicate().resultPredicate().orElse(ignore -> false);
    }

    @Override
    public void close() throws Exception {
        userLookupFunction.close();
        super.close();
    }
}
