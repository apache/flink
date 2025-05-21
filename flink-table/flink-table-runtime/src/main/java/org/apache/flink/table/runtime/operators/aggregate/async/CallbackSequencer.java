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

/**
 * Sequences callbacks by registering and then invoking them.
 *
 * @param <D> Metadata passed along to calls. Useful for passing arguments to callbacks.
 * @param <C> Context object which has a timer and mailbox available.
 */
public interface CallbackSequencer<D, C extends KeyedAsyncFunction.OpenContext> {

    /** A callback which can be invoked on a per-key basis. */
    interface Callback<D, C> {
        void callback(long timestamp, D data, C ctx) throws Exception;
    }

    void callbackWhenNext(C ctx, long timestamp) throws Exception;

    /**
     * Adds the caller to the queue and invokes the callback when they are at the front.
     *
     * @param ctx The context to store.
     * @param timestamp The timestamp associated with the callback
     * @param metadata Metadata to be passed along with the callback
     */
    void callbackWhenNext(C ctx, long timestamp, D metadata) throws Exception;

    /**
     * Invokes the callback for the next waiter.
     *
     * @param ctx The context
     */
    void notifyNextWaiter(C ctx) throws Exception;
}
