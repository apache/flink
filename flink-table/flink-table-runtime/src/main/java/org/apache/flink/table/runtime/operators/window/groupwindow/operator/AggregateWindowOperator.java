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

package org.apache.flink.table.runtime.operators.window.groupwindow.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.groupwindow.assigners.GroupWindowAssigner;
import org.apache.flink.table.runtime.operators.window.groupwindow.triggers.Trigger;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link WindowOperator} for grouped window aggregates.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link GroupWindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same {@code
 * Window}. An element can be in multiple panes if it was assigned to multiple windows by the {@code
 * WindowAssigner}.
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires, the given
 * {@link NamespaceAggsHandleFunction#getValue(Object)} is invoked to produce the results that are
 * emitted for the pane to which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class AggregateWindowOperator<K, W extends Window> extends WindowOperator<K, W> {

    private static final long serialVersionUID = 1L;

    private NamespaceAggsHandleFunction<W> aggWindowAggregator;
    private GeneratedNamespaceAggsHandleFunction<W> generatedAggWindowAggregator;

    private transient JoinedRowData reuseOutput;

    /**
     * The util to compare two RowData equals to each other. As different RowData can't be equals
     * directly, we use a code generated util to handle this.
     */
    protected RecordEqualiser equaliser;

    private GeneratedRecordEqualiser generatedEqualiser;

    AggregateWindowOperator(
            NamespaceAggsHandleFunction<W> windowAggregator,
            RecordEqualiser equaliser,
            GroupWindowAssigner<W> windowAssigner,
            Trigger<W> trigger,
            TypeSerializer<W> windowSerializer,
            LogicalType[] inputFieldTypes,
            LogicalType[] accumulatorTypes,
            LogicalType[] aggResultTypes,
            LogicalType[] windowPropertyTypes,
            int rowtimeIndex,
            boolean produceUpdates,
            long allowedLateness,
            ZoneId shiftTimeZone,
            int inputCountIndex) {
        super(
                windowAggregator,
                windowAssigner,
                trigger,
                windowSerializer,
                inputFieldTypes,
                accumulatorTypes,
                aggResultTypes,
                windowPropertyTypes,
                rowtimeIndex,
                produceUpdates,
                allowedLateness,
                shiftTimeZone,
                inputCountIndex);
        this.aggWindowAggregator = windowAggregator;
        this.equaliser = checkNotNull(equaliser);
    }

    AggregateWindowOperator(
            GeneratedNamespaceAggsHandleFunction<W> generatedAggWindowAggregator,
            GeneratedRecordEqualiser generatedEqualiser,
            GroupWindowAssigner<W> windowAssigner,
            Trigger<W> trigger,
            TypeSerializer<W> windowSerializer,
            LogicalType[] inputFieldTypes,
            LogicalType[] accumulatorTypes,
            LogicalType[] aggResultTypes,
            LogicalType[] windowPropertyTypes,
            int rowtimeIndex,
            boolean sendRetraction,
            long allowedLateness,
            ZoneId shiftTimeZone,
            int inputCountIndex) {
        super(
                windowAssigner,
                trigger,
                windowSerializer,
                inputFieldTypes,
                accumulatorTypes,
                aggResultTypes,
                windowPropertyTypes,
                rowtimeIndex,
                sendRetraction,
                allowedLateness,
                shiftTimeZone,
                inputCountIndex);
        this.generatedAggWindowAggregator = generatedAggWindowAggregator;
        this.generatedEqualiser = checkNotNull(generatedEqualiser);
    }

    @Override
    public void open() throws Exception {
        super.open();
        reuseOutput = new JoinedRowData();
    }

    @Override
    protected void compileGeneratedCode() {
        // compile aggregator
        if (generatedAggWindowAggregator != null) {
            aggWindowAggregator =
                    generatedAggWindowAggregator.newInstance(
                            getRuntimeContext().getUserCodeClassLoader());
            windowAggregator = aggWindowAggregator;
        }

        // compile equaliser
        if (generatedEqualiser != null) {
            equaliser =
                    generatedEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());
        }
    }

    @Override
    protected void emitWindowResult(W window) throws Exception {
        windowFunction.prepareAggregateAccumulatorForEmit(window);
        RowData acc = aggWindowAggregator.getAccumulators();
        RowData aggResult = aggWindowAggregator.getValue(window);
        if (produceUpdates) {
            previousState.setCurrentNamespace(window);
            RowData previousAggResult = previousState.value();

            if (!recordCounter.recordCountIsZero(acc)) {
                // has emitted result for the window
                if (previousAggResult != null) {
                    // current agg is not equal to the previous emitted, should emit retract
                    if (!equaliser.equals(aggResult, previousAggResult)) {
                        // send UPDATE_BEFORE
                        collect(
                                RowKind.UPDATE_BEFORE,
                                (RowData) getCurrentKey(),
                                previousAggResult);
                        // send UPDATE_AFTER
                        collect(RowKind.UPDATE_AFTER, (RowData) getCurrentKey(), aggResult);
                        // update previousState
                        previousState.update(aggResult);
                    }
                    // if the previous agg equals to the current agg, no need to send retract and
                    // accumulate
                }
                // the first fire for the window, only send INSERT
                else {
                    // send INSERT
                    collect(RowKind.INSERT, (RowData) getCurrentKey(), aggResult);
                    // update previousState
                    previousState.update(aggResult);
                }
            } else {
                // has emitted result for the window
                // we retracted the last record for this key
                if (previousAggResult != null) {
                    // send DELETE
                    collect(RowKind.DELETE, (RowData) getCurrentKey(), previousAggResult);
                    // clear previousState
                    previousState.clear();
                }
                // if the counter is zero, no need to send accumulate
            }
        } else {
            if (!recordCounter.recordCountIsZero(acc)) {
                // send INSERT
                collect(RowKind.INSERT, (RowData) getCurrentKey(), aggResult);
            }
            // if the counter is zero, no need to send accumulate
            // there is no possible skip `if` branch when `produceUpdates` is false
        }
    }

    private void collect(RowKind rowKind, RowData key, RowData aggResult) {
        reuseOutput.replace((RowData) getCurrentKey(), aggResult);
        reuseOutput.setRowKind(rowKind);
        collector.collect(reuseOutput);
    }
}
