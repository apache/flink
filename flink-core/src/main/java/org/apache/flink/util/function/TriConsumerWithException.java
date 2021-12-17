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

import org.apache.flink.util.ExceptionUtils;

/**
 * A checked extension of the {@link TriConsumer} interface.
 *
 * @param <S> type of the first argument
 * @param <T> type of the first argument
 * @param <U> type of the second argument
 * @param <E> type of the thrown exception
 */
@FunctionalInterface
public interface TriConsumerWithException<S, T, U, E extends Throwable> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param s the first input argument
     * @param t the second input argument
     * @param u the third input argument
     * @throws E in case of an error
     */
    void accept(S s, T t, U u) throws E;

    /**
     * Convert a {@link TriConsumerWithException} into a {@link TriConsumer}.
     *
     * @param triConsumerWithException TriConsumer with exception to convert into a {@link
     *     TriConsumer}.
     * @param <A> first input type
     * @param <B> second input type
     * @param <C> third input type
     * @return {@link TriConsumer} which rethrows all checked exceptions as unchecked.
     */
    static <A, B, C> TriConsumer<A, B, C> unchecked(
            TriConsumerWithException<A, B, C, ?> triConsumerWithException) {
        return (A a, B b, C c) -> {
            try {
                triConsumerWithException.accept(a, b, c);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            }
        };
    }
}
