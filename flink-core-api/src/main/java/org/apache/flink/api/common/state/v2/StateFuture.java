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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

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
    <U> StateFuture<U> thenApply(
            FunctionWithException<? super T, ? extends U, ? extends Exception> fn);

    /**
     * Returns a new StateFuture that, when this future completes normally, is executed with this
     * future's result as the argument to the supplied action.
     *
     * @param action the action to perform before completing the returned StateFuture.
     * @return the new StateFuture.
     */
    StateFuture<Void> thenAccept(ThrowingConsumer<? super T, ? extends Exception> action);

    /**
     * Returns a new future that, when this future completes normally, is executed with this future
     * as the argument to the supplied function.
     *
     * @param action the action to perform.
     * @return the new StateFuture.
     */
    <U> StateFuture<U> thenCompose(
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception> action);

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
            StateFuture<? extends U> other,
            BiFunctionWithException<? super T, ? super U, ? extends V, ? extends Exception> fn);

    /**
     * Apply a condition test on the result of this StateFuture, and try to perform one action out
     * of two based on the result. Gather the results of the condition test and the selected action
     * into a StateFuture of tuple. The relationship between the action result and the returned new
     * StateFuture are just like the {@link #thenApply(FunctionWithException)}.
     *
     * @param condition the condition test.
     * @param actionIfTrue the function to apply if the condition returns true.
     * @param actionIfFalse the function to apply if the condition returns false.
     * @param <U> the type of the output from actionIfTrue.
     * @param <V> the type of the output from actionIfFalse.
     * @return the new StateFuture with the result of condition test, and result of action.
     */
    <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue,
            FunctionWithException<? super T, ? extends V, ? extends Exception> actionIfFalse);

    /**
     * Apply a condition test on the result of this StateFuture, and try to perform the action if
     * test result is true. Gather the results of the condition test and the action (if applied)
     * into a StateFuture of tuple. The relationship between the action result and the returned new
     * StateFuture are just like the {@link #thenApply(FunctionWithException)}.
     *
     * @param condition the condition test.
     * @param actionIfTrue the function to apply if the condition returns true.
     * @param <U> the type of the output from actionIfTrue.
     * @return the new StateFuture with the result of condition test, and result of action.
     */
    <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue);

    /**
     * Apply a condition test on the result of this StateFuture, and try to perform one action out
     * of two based on the result. Gather the results of the condition test StateFuture.
     *
     * @param condition the condition test.
     * @param actionIfTrue the function to apply if the condition returns true.
     * @param actionIfFalse the function to apply if the condition returns false.
     * @return the new StateFuture.
     */
    StateFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue,
            ThrowingConsumer<? super T, ? extends Exception> actionIfFalse);

    /**
     * Apply a condition test on the result of this StateFuture, and try to perform the action if
     * test result is true. Gather the results of the condition test StateFuture.
     *
     * @param condition the condition test.
     * @param actionIfTrue the function to apply if the condition returns true.
     * @return the new StateFuture.
     */
    StateFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue);

    /**
     * Apply a condition test on the result of this StateFuture, and try to perform one action out
     * of two based on the result. Gather the results of the condition test and the selected action
     * into a StateFuture of tuple. The relationship between the action result and the returned new
     * StateFuture are just like the {@link #thenCompose(FunctionWithException)}.
     *
     * @param condition the condition test.
     * @param actionIfTrue the function to apply if the condition returns true.
     * @param actionIfFalse the function to apply if the condition returns false.
     * @param <U> the type of the output from actionIfTrue.
     * @param <V> the type of the output from actionIfFalse.
     * @return the new StateFuture with the result of condition test, and result of action.
     */
    <U, V> StateFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue,
            FunctionWithException<? super T, ? extends StateFuture<V>, ? extends Exception>
                    actionIfFalse);

    /**
     * Apply a condition test on the result of this StateFuture, and try to perform the action if
     * test result is true. Gather the results of the condition test and the action (if applied)
     * into a StateFuture of tuple. The relationship between the action result and the returned new
     * StateFuture are just like the {@link #thenCompose(FunctionWithException)}
     * (FunctionWithException)}.
     *
     * @param condition the condition test.
     * @param actionIfTrue the function to apply if the condition returns true.
     * @param <U> the type of the output from actionIfTrue.
     * @return the new StateFuture with the result of condition test, and result of action.
     */
    <U> StateFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue);
}
