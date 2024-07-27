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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

/**
 * Async function runner for {@link org.apache.flink.table.functions.AsyncScalarFunction}, which
 * takes the generated function, instantiates it, and then calls its lifecycle methods.
 */
public class AsyncFunctionRunner extends RichAsyncFunction<RowData, RowData> {

    private static final long serialVersionUID = -6664660022391632123L;

    private final GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher;

    private transient AsyncFunction<RowData, RowData> fetcher;

    public AsyncFunctionRunner(
            GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher) {
        this.generatedFetcher = generatedFetcher;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) {
        try {
            fetcher.asyncInvoke(input, resultFuture);
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(fetcher);
    }
}
