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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.ListenableCollector;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

/** The join runner with an additional calculate function on the dimension table. */
public class LookupJoinWithCalcRunner extends LookupJoinRunner {

    private static final long serialVersionUID = 5277183384939603386L;
    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc;

    private transient FlatMapFunction<RowData, RowData> calc;
    private transient Collector<RowData> calcCollector;

    public LookupJoinWithCalcRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
            GeneratedCollector<ListenableCollector<RowData>> generatedCollector,
            GeneratedFunction<FilterCondition> generatedFilterCondition,
            boolean isLeftOuterJoin,
            int tableFieldsCount) {
        super(
                generatedFetcher,
                generatedCollector,
                generatedFilterCondition,
                isLeftOuterJoin,
                tableFieldsCount);
        this.generatedCalc = generatedCalc;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.calc = generatedCalc.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(calc, getRuntimeContext());
        FunctionUtils.openFunction(calc, openContext);
        this.calcCollector = new CalcCollector(collector);
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(calc);
    }

    @Override
    public Collector<RowData> getFetcherCollector() {
        return calcCollector;
    }

    private class CalcCollector implements Collector<RowData> {

        private final Collector<RowData> delegate;

        private CalcCollector(Collector<RowData> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void collect(RowData record) {
            try {
                calc.flatMap(record, delegate);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
