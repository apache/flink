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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

/** The join runner to lookup the dimension table. */
public class LookupJoinRunner extends ProcessFunction<RowData, RowData> {
    private static final long serialVersionUID = -4521543015709964733L;

    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher;
    private final GeneratedCollector<ListenableCollector<RowData>> generatedCollector;
    private final GeneratedFunction<FilterCondition> generatedPreFilterCondition;

    protected final boolean isLeftOuterJoin;
    protected final int tableFieldsCount;

    private transient FlatMapFunction<RowData, RowData> fetcher;
    protected transient ListenableCollector<RowData> collector;
    protected transient JoinedRowData outRow;
    protected transient FilterCondition preFilterCondition;
    private transient GenericRowData nullRow;

    public LookupJoinRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedCollector<ListenableCollector<RowData>> generatedCollector,
            GeneratedFunction<FilterCondition> generatedPreFilterCondition,
            boolean isLeftOuterJoin,
            int tableFieldsCount) {
        this.generatedFetcher = generatedFetcher;
        this.generatedCollector = generatedCollector;
        this.generatedPreFilterCondition = generatedPreFilterCondition;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.tableFieldsCount = tableFieldsCount;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.collector =
                generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.preFilterCondition =
                generatedPreFilterCondition.newInstance(
                        getRuntimeContext().getUserCodeClassLoader());

        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(preFilterCondition, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);
        FunctionUtils.openFunction(collector, openContext);
        FunctionUtils.openFunction(preFilterCondition, openContext);

        this.nullRow = new GenericRowData(tableFieldsCount);
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
        // apply local filter first
        if (preFilterCondition.apply(in)) {
            // fetcher has copied the input field when object reuse is enabled
            fetcher.flatMap(in, getFetcherCollector());
        }
    }

    public void padNullForLeftJoin(RowData in, Collector<RowData> out) {
        if (isLeftOuterJoin && !collector.isCollected()) {
            outRow.replace(in, nullRow);
            outRow.setRowKind(in.getRowKind());
            out.collect(outRow);
        }
    }

    public Collector<RowData> getFetcherCollector() {
        return collector;
    }

    @Override
    public void close() throws Exception {
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (collector != null) {
            FunctionUtils.closeFunction(collector);
        }
        if (preFilterCondition != null) {
            FunctionUtils.closeFunction(preFilterCondition);
        }
        super.close();
    }
}
