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

package org.apache.flink.table.runtime.operators.sort.asyncprocessing;

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.ListStateDescriptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.utils.ProcTimeSortHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Sort on proc-time and additional secondary sort attributes in async state. */
public class AsyncStateProcTimeSortOperator extends AsyncStateBaseTemporalSortOperator {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncStateProcTimeSortOperator.class);

    private final InternalTypeInfo<RowData> inputRowType;

    private GeneratedRecordComparator gComparator;
    private transient RecordComparator comparator;
    private transient List<RowData> sortBuffer;

    private transient ListState<RowData> dataState;

    private transient AsyncProcTimeSortHelper sortHelper = null;

    /**
     * @param inputRowType The data type of the input data.
     * @param gComparator generated comparator.
     */
    public AsyncStateProcTimeSortOperator(
            InternalTypeInfo<RowData> inputRowType, GeneratedRecordComparator gComparator) {
        this.inputRowType = inputRowType;
        this.gComparator = gComparator;
    }

    @Override
    public void open() throws Exception {
        super.open();

        LOG.info("Opening ProcTimeSortOperator");

        comparator = gComparator.newInstance(getContainingTask().getUserCodeClassLoader());
        gComparator = null;
        sortBuffer = new ArrayList<>();

        ListStateDescriptor<RowData> sortDescriptor =
                new ListStateDescriptor<>("sortState", inputRowType);
        dataState = getRuntimeContext().getListState(sortDescriptor);

        sortHelper = new AsyncProcTimeSortHelper();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        long currentTime = timerService.currentProcessingTime();

        // buffer the event incoming event
        dataState.add(input);

        // register a timer for the next millisecond to sort and emit buffered data
        timerService.registerProcessingTimeTimer(currentTime + 1);
    }

    @Override
    public void onProcessingTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {

        // gets all rows for the triggering timestamps
        StateFuture<StateIterator<RowData>> inputsFuture = dataState.asyncGet();

        inputsFuture.thenAccept(
                inputsIterator -> {
                    // insert all rows into the sort buffer
                    sortBuffer.clear();
                    inputsIterator.onNext(
                            (RowData row) -> {
                                sortBuffer.add(row);
                            });

                    sortHelper.emitData(sortBuffer);
                });
    }

    @Override
    public void onEventTime(InternalTimer<RowData, VoidNamespace> timer) throws Exception {
        throw new UnsupportedOperationException(
                "Now Sort only is supported based processing time here!");
    }

    private class AsyncProcTimeSortHelper extends ProcTimeSortHelper {

        public AsyncProcTimeSortHelper() {
            super(sortBuffer, comparator, AsyncStateProcTimeSortOperator.this.collector);
        }

        @Override
        protected void clearDataState() {
            // no need to wait for the future
            // remove all buffered rows
            dataState.clear();
        }
    }
}
