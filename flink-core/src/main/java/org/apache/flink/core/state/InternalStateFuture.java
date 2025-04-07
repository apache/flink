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

package org.apache.flink.core.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * The Internal definition of {@link StateFuture}, add some method that will be used by framework.
 */
@Internal
public interface InternalStateFuture<T> extends StateFuture<T> {
    /**
     * Returns {@code true} if completed in any fashion: normally, exceptionally, or via
     * cancellation.
     *
     * @return {@code true} if completed
     */
    boolean isDone();

    /** Waits if necessary for the computation to complete, and then retrieves its result. */
    T get();

    /** Complete this future. */
    void complete(T result);

    /**
     * Fail this future and pass the given exception to the runtime.
     *
     * @param message the description of this exception
     * @param ex the exception
     */
    void completeExceptionally(String message, Throwable ex);

    /**
     * Accept the action in the same thread with the one of complete (or current thread if it has
     * been completed).
     *
     * @param action the action to perform.
     */
    void thenSyncAccept(ThrowingConsumer<? super T, ? extends Exception> action);
}
