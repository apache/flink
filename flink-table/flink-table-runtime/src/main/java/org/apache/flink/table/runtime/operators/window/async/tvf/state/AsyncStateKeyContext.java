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

package org.apache.flink.table.runtime.operators.window.async.tvf.state;

import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.ThrowingRunnable;

/** Context to switch current key in async state backend. */
public class AsyncStateKeyContext {

    private final AbstractAsyncStateStreamOperator<RowData> asyncStateProcessingOperator;

    private final AsyncKeyedStateBackend<?> asyncKeyedStateBackend;

    public AsyncStateKeyContext(
            AbstractAsyncStateStreamOperator<RowData> asyncStateProcessingOperator,
            AsyncKeyedStateBackend<?> asyncKeyedStateBackend) {
        this.asyncStateProcessingOperator = asyncStateProcessingOperator;
        this.asyncKeyedStateBackend = asyncKeyedStateBackend;
    }

    public void asyncProcessWithKey(RowData key, ThrowingRunnable<Exception> processing) {
        asyncStateProcessingOperator.asyncProcessWithKey(key, processing);
    }

    public AsyncKeyedStateBackend<?> getAsyncKeyedStateBackend() {
        return asyncKeyedStateBackend;
    }

    public RowData getCurrentKey() {
        return (RowData) asyncStateProcessingOperator.getCurrentKey();
    }
}
