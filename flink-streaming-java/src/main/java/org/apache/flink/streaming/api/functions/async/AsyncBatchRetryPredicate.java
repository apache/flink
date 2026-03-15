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

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Interface that encapsulates predicates for determining when to retry a batch async operation.
 *
 * <p>This is the batch-equivalent of {@link AsyncRetryPredicate}, designed specifically for {@link
 * AsyncBatchFunction} operations.
 *
 * @param <OUT> The type of the output elements.
 */
@PublicEvolving
public interface AsyncBatchRetryPredicate<OUT> {

    /**
     * An Optional Java {@link Predicate} that defines a condition on the batch function's result
     * which will trigger a retry operation.
     *
     * <p>This predicate is evaluated on the complete collection of results returned by {@link
     * ResultFuture#complete(Collection)}.
     *
     * @return predicate on result of {@link Collection}, or empty if no result-based retry is
     *     configured
     */
    Optional<Predicate<Collection<OUT>>> resultPredicate();

    /**
     * An Optional Java {@link Predicate} that defines a condition on the batch function's exception
     * which will trigger a retry operation.
     *
     * <p>This predicate is evaluated on the exception passed to {@link
     * ResultFuture#completeExceptionally(Throwable)}.
     *
     * @return predicate on {@link Throwable} exception, or empty if no exception-based retry is
     *     configured
     */
    Optional<Predicate<Throwable>> exceptionPredicate();
}
