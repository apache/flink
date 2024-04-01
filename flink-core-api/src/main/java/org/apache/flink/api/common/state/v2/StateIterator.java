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

package org.apache.flink.api.common.state.v2;

import org.apache.flink.annotation.Experimental;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Asynchronous iterators allow to iterate over data that comes asynchronously, on-demand.
 *
 * @param <T> The element type of this iterator.
 */
@Experimental
public interface StateIterator<T> {
    /**
     * Async iterate the data and call the callback when data is ready.
     *
     * @param iterating the data action when it is ready. The return is the state future for
     *     chaining.
     * @param <U> the type of the inner returned StateFuture's result.
     * @return the Future that will trigger when this iterator and all returned state future get its
     *     results.
     */
    <U> StateFuture<Collection<U>> onNext(Function<T, StateFuture<? extends U>> iterating);

    /**
     * Async iterate the data and call the callback when data is ready.
     *
     * @param iterating the data action when it is ready.
     * @return the Future that will trigger when this iterator ends.
     */
    StateFuture<Void> onNext(Consumer<T> iterating);

    /** Return if this iterator is empty synchronously. */
    boolean isEmpty();
}
