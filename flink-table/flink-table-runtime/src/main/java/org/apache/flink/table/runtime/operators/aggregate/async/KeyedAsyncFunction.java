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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.util.function.ThrowingRunnable;

/** An {@link AsyncFunction} that is meant to work on keyed elements. */
public interface KeyedAsyncFunction<K, IN, OUT> extends AsyncFunction<IN, OUT> {

    /**
     * Opens the function. This method is called before any methods are invoked.
     *
     * @param context The context for this function.
     */
    default void open(OpenContext context) throws Exception {}

    /** Closes the function. This method is called after all methods have been invoked. */
    default void close() throws Exception {}

    /** Context passed to {@link #open(OpenContext)}. */
    interface OpenContext extends ExecutionContext {
        /**
         * Runs the given runnable on the mailbox thread. This is useful to writing single-threaded
         * functions so that async results can be processed on the same thread which calls
         * asyncInvoke and onTimer.
         */
        void runOnMailboxThread(ThrowingRunnable<Exception> runnable);
    }
}
