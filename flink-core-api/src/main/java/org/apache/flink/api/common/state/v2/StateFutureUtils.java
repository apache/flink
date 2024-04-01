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

/** A collection of utilities that expand the usage of {@link StateFuture}. */
@Experimental
public class StateFutureUtils {
    /** Returns a completed future that does nothing and return null. */
    public static <V> StateFuture<V> completedVoidFuture() {
        throw new UnsupportedOperationException("To be implemented.");
    }

    /** Returns a completed future that does nothing and return provided result. */
    public static <V> StateFuture<V> completedFuture(V result) {
        throw new UnsupportedOperationException("To be implemented.");
    }

    /**
     * Creates a future that is complete once multiple other futures completed. Upon successful
     * completion, the future returns the collection of the futures' results.
     *
     * @param futures The futures that make up the conjunction. No null entries are allowed,
     *     otherwise a IllegalArgumentException will be thrown.
     * @return The StateFuture that completes once all given futures are complete.
     */
    public static <T> StateFuture<Collection<T>> combineAll(
            Collection<? extends StateFuture<? extends T>> futures) {
        throw new UnsupportedOperationException("To be implemented.");
    }
}
