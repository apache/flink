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

package org.apache.flink.table.runtime.operators.aggregate.window.combines;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.UnsupportedStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.util.WindowKey;
import org.apache.flink.util.Collector;

import java.util.Iterator;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;

/**
 * An implementation of {@link RecordsCombiner} that accumulates input records into local
 * accumulators.
 *
 * <p>Note: this only supports event-time window.
 */
public class LocalAggCombiner implements RecordsCombiner {

    /** Function used to handle all aggregates. */
    private final NamespaceAggsHandleFunction<Long> aggregator;

    /** The output to emit combined accumulator result. */
    private final Collector<RowData> collector;

    /** Reused output row, the structure is Join(key, accWindow). */
    private final JoinedRowData resultRow = new JoinedRowData();

    /** Reused row consisted of acc and window row, the structure is Join(acc, window). */
    private final JoinedRowData accWindowRow = new JoinedRowData();

    /**
     * Reused window row, the window is represented by a long value which represents slice end or
     * window end.
     */
    private final GenericRowData windowRow = new GenericRowData(1);

    public LocalAggCombiner(
            NamespaceAggsHandleFunction<Long> aggregator, Collector<RowData> collector) {
        this.aggregator = aggregator;
        this.collector = collector;
    }

    @Override
    public void combine(WindowKey windowKey, Iterator<RowData> records) throws Exception {
        // always not copy key/value because they are not cached.
        final RowData key = windowKey.getKey();
        final Long window = windowKey.getWindow();

        // step 1: create an empty accumulator
        RowData acc = aggregator.createAccumulators();

        // step 2: set accumulator to function
        aggregator.setAccumulators(window, acc);

        // step 3: do accumulate
        while (records.hasNext()) {
            RowData record = records.next();
            if (isAccumulateMsg(record)) {
                aggregator.accumulate(record);
            } else {
                aggregator.retract(record);
            }
        }

        // step 4: get accumulator and output accumulator
        acc = aggregator.getAccumulators();
        output(key, window, acc);
    }

    @Override
    public void close() throws Exception {
        aggregator.close();
    }

    private void output(RowData key, Long window, RowData acc) {
        // consist a row in structure of (key_row, acc_row, window_val)
        windowRow.setField(0, window);
        accWindowRow.replace(acc, windowRow);
        resultRow.replace(key, accWindowRow);
        collector.collect(resultRow);
    }

    // ----------------------------------------------------------------------------------------
    // Factory
    // ----------------------------------------------------------------------------------------

    /** Factory to create {@link LocalAggCombiner}. */
    public static final class Factory implements RecordsCombiner.LocalFactory {

        private static final long serialVersionUID = 1L;

        private final GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler;

        public Factory(GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler) {
            this.genAggsHandler = genAggsHandler;
        }

        @Override
        public RecordsCombiner createRecordsCombiner(
                RuntimeContext runtimeContext, Collector<RowData> collector) throws Exception {
            final NamespaceAggsHandleFunction<Long> aggregator =
                    genAggsHandler.newInstance(runtimeContext.getUserCodeClassLoader());
            aggregator.open(new UnsupportedStateDataViewStore(runtimeContext));
            return new LocalAggCombiner(aggregator, collector);
        }
    }
}
