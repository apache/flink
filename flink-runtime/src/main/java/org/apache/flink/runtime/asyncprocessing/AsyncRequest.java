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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;

@SuppressWarnings("rawtypes")
public abstract class AsyncRequest<K> {

    /** The record context of this request. */
    protected final RecordContext<K> context;

    protected final boolean sync;

    /** The future to collect the result of the request. */
    protected final InternalAsyncFuture asyncFuture;

    public AsyncRequest(RecordContext<K> context, boolean sync, InternalAsyncFuture asyncFuture) {
        this.context = context;
        this.sync = sync;
        this.asyncFuture = asyncFuture;
    }

    public RecordContext<K> getRecordContext() {
        return context;
    }

    public boolean isSync() {
        return sync;
    }

    public InternalAsyncFuture getFuture() {
        return asyncFuture;
    }
}
