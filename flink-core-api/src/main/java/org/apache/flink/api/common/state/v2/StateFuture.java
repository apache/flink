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

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * StateFuture is a future that act as a return value for async state interfaces. Note: All these
 * methods of this interface can ONLY be called within task thread.
 *
 * @param <T> The return type of this future.
 */
@Experimental
public interface StateFuture<T> {
    /**
     * Returns a new StateFuture that, when this future completes normally, is executed with this
     * future's result as the argument to the supplied function.
     *
     * @param fn the function to use to compute the value of the returned StateFuture.
     * @param <U> the function's return type.
     * @return the new StateFuture.
     */
    <U> StateFuture<U> thenApply(Function<? super T, ? extends U> fn);

    /**
     * Returns a new StateFuture that, when this future completes normally, is executed with this
     * future's result as the argument to the supplied action.
     *
     * @param action the action to perform before completing the returned StateFuture.
     * @return the new StateFuture.
     */
    StateFuture<Void> thenAccept(Consumer<? super T> action);

    /**
     * Returns a new future that, when this future completes normally, is executed with this future
     * as the argument to the supplied function.
     *
     * @param action the action to perform.
     * @return the new StateFuture.
     */
    <U> StateFuture<U> thenCompose(Function<? super T, ? extends StateFuture<U>> action);

    /**
     * Returns a new StateFuture that, when this and the other given future both complete normally,
     * is executed with the two results as arguments to the supplied function.
     *
     * @param other the other StateFuture.
     * @param fn the function to use to compute the value of the returned StateFuture.
     * @param <U> the type of the other StateFuture's result.
     * @param <V> the function's return type.
     * @return the new StateFuture.
     */
    <U, V> StateFuture<V> thenCombine(
            StateFuture<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);
}
