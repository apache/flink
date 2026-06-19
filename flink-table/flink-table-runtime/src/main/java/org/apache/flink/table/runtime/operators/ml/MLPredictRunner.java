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

package org.apache.flink.table.runtime.operators.ml;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.PredictFunction;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.AbstractFunctionRunner;
import org.apache.flink.util.Collector;

/**
 * Function runner for {@link PredictFunction}, which takes the generated function, instantiates it,
 * and then calls its lifecycle methods.
 */
public class MLPredictRunner extends AbstractFunctionRunner {

    private final GeneratedCollector<ListenableCollector<RowData>> generatedCollector;

    protected transient ListenableCollector<RowData> collector;

    public MLPredictRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedCollector<ListenableCollector<RowData>> generatedCollector) {
        super(generatedFetcher);
        this.generatedCollector = generatedCollector;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        this.collector =
                generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
        FunctionUtils.openFunction(collector, openContext);
    }

    @Override
    public void processElement(
            RowData in, ProcessFunction<RowData, RowData>.Context ctx, Collector<RowData> out)
            throws Exception {
        prepareCollector(in, out);
        fetcher.flatMap(in, collector);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (collector != null) {
            FunctionUtils.closeFunction(collector);
        }
    }

    public void prepareCollector(RowData in, Collector<RowData> out) {
        collector.setCollector(out);
        collector.setInput(in);
        collector.reset();
    }
}
