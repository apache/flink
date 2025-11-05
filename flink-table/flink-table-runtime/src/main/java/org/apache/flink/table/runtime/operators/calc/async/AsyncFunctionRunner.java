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

package org.apache.flink.table.runtime.operators.calc.async;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.AbstractAsyncFunctionRunner;

/**
 * Async function runner for {@link org.apache.flink.table.functions.AsyncScalarFunction}, which
 * takes the generated function, instantiates it, and then calls its lifecycle methods.
 */
public class AsyncFunctionRunner extends AbstractAsyncFunctionRunner<RowData> {

    private static final long serialVersionUID = -7198305381139008806L;

    public AsyncFunctionRunner(
            GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher) {
        super(generatedFetcher);
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
        try {
            fetcher.asyncInvoke(input, resultFuture);
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }
}
