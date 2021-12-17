/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util.function;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.ExceptionUtils;

import java.util.function.BiFunction;

/**
 * Function which takes three arguments.
 *
 * @param <S> type of the first argument
 * @param <T> type of the second argument
 * @param <U> type of the third argument
 * @param <R> type of the return value
 * @param <E> type of the thrown exception
 */
@PublicEvolving
@FunctionalInterface
public interface TriFunctionWithException<S, T, U, R, E extends Throwable> {

    /**
     * Applies this function to the given arguments.
     *
     * @param s the first function argument
     * @param t the second function argument
     * @param u the third function argument
     * @return the function result
     * @throws E if it fails
     */
    R apply(S s, T t, U u) throws E;

    /**
     * Convert at {@link TriFunctionWithException} into a {@link TriFunction}.
     *
     * @param triFunctionWithException function with exception to convert into a function
     * @param <A> first input type
     * @param <B> second input type
     * @param <C> third input type
     * @param <D> output type
     * @return {@link BiFunction} which throws all checked exception as an unchecked exception.
     */
    static <A, B, C, D> TriFunction<A, B, C, D> unchecked(
            TriFunctionWithException<A, B, C, D, ?> triFunctionWithException) {
        return (A a, B b, C c) -> {
            try {
                return triFunctionWithException.apply(a, b, c);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
                // we need this to appease the compiler :-(
                return null;
            }
        };
    }
}
