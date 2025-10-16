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

package org.apache.flink.table.runtime.operators.search;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.AbstractFunctionRunner;
import org.apache.flink.util.Collector;

/** The runner to perform vector search. */
public class VectorSearchRunner extends AbstractFunctionRunner {

    private static final long serialVersionUID = 1L;
    private final GeneratedCollector<ListenableCollector<RowData>> generatedCollector;
    private final boolean isLeftOuterJoin;
    private final int searchTableFieldCount;
    private transient ListenableCollector<RowData> collector;
    private transient JoinedRowData outRow;
    private transient GenericRowData nullRow;

    public VectorSearchRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedCollector<ListenableCollector<RowData>> generatedCollector,
            boolean isLeftOuterJoin,
            int searchTableFieldCount) {
        super(generatedFetcher);
        this.generatedCollector = generatedCollector;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.searchTableFieldCount = searchTableFieldCount;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.collector =
                generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
        FunctionUtils.openFunction(collector, openContext);
        this.nullRow = new GenericRowData(searchTableFieldCount);
        this.outRow = new JoinedRowData();
    }

    @Override
    public void processElement(RowData in, Context ctx, Collector<RowData> out) throws Exception {
        prepareCollector(in, out);
        doFetch(in);
        padNullForLeftJoin(in, out);
    }

    public void prepareCollector(RowData in, Collector<RowData> out) {
        collector.setCollector(out);
        collector.setInput(in);
        collector.reset();
    }

    public void doFetch(RowData in) throws Exception {
        // fetcher has copied the input field when object reuse is enabled
        fetcher.flatMap(in, collector);
    }

    public void padNullForLeftJoin(RowData in, Collector<RowData> out) {
        if (isLeftOuterJoin && !collector.isCollected()) {
            outRow.replace(in, nullRow);
            outRow.setRowKind(in.getRowKind());
            out.collect(outRow);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (collector != null) {
            FunctionUtils.closeFunction(collector);
        }
    }
}
