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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

/**
 * Base function runner for specialized table function, e.g. {@link AsyncLookupFunction} or {@link
 * AsyncPredictFunction}.
 */
public abstract class AbstractAsyncFunctionRunner<T> extends RichAsyncFunction<RowData, RowData> {

    protected final GeneratedFunction<AsyncFunction<RowData, T>> generatedFetcher;

    protected transient AsyncFunction<RowData, T> fetcher;

    public AbstractAsyncFunctionRunner(
            GeneratedFunction<AsyncFunction<RowData, T>> generatedFetcher) {
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
    public void close() throws Exception {
        super.close();
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
    }
}
