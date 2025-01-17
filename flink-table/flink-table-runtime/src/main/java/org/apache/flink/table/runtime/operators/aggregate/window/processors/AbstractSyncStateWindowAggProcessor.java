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

package org.apache.flink.table.runtime.operators.aggregate.window.processors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.tvf.common.SyncStateWindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners;
import org.apache.flink.table.runtime.operators.window.tvf.state.WindowValueState;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A base class for window aggregate processors. */
public abstract class AbstractSyncStateWindowAggProcessor<W>
        extends WindowAggProcessorBase<W, SyncStateWindowProcessor.SyncStateContext<W>>
        implements SyncStateWindowProcessor<W> {

    private static final long serialVersionUID = 1L;

    protected final WindowIsEmptySupplier emptySupplier;

    // ----------------------------------------------------------------------------------------

    /** state schema: [key, window, accumulator]. */
    protected transient WindowValueState<W> windowState;

    public AbstractSyncStateWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<W> genAggsHandler,
            WindowAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer,
            boolean isEventTime,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(genAggsHandler, accSerializer, isEventTime, shiftTimeZone);
        this.emptySupplier = new WindowIsEmptySupplier(indexOfCountStar, sliceAssigner);
    }

    @Override
    public void open(SyncStateContext<W> context) throws Exception {
        super.open(context);

        ValueState<RowData> state =
                ctx.getKeyedStateBackend()
                        .getOrCreateKeyedState(
                                createWindowSerializer(),
                                new ValueStateDescriptor<>("window-aggs", accSerializer));
        this.windowState = new WindowValueState<>((InternalValueState<RowData, W, RowData>) state);
    }

    @Override
    protected final void prepareAggregator() throws Exception {
        this.aggregator =
                genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        this.aggregator.open(
                new PerWindowStateDataViewStore(
                        ctx.getKeyedStateBackend(),
                        createWindowSerializer(),
                        ctx.getRuntimeContext()));
    }

    protected void collect(RowData aggResult) {
        collect(ctx.getKeyedStateBackend().getCurrentKey(), aggResult);
    }

    /** A supplier that returns whether the window is empty. */
    protected final class WindowIsEmptySupplier implements Supplier<Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        private final int indexOfCountStar;

        private WindowIsEmptySupplier(int indexOfCountStar, WindowAssigner assigner) {
            if (assigner instanceof SliceAssigners.HoppingSliceAssigner) {
                checkArgument(
                        indexOfCountStar >= 0,
                        "Hopping window requires a COUNT(*) in the aggregate functions.");
            }
            this.indexOfCountStar = indexOfCountStar;
        }

        @Override
        public Boolean get() {
            if (indexOfCountStar < 0) {
                return false;
            }
            try {
                RowData acc = aggregator.getAccumulators();
                return acc == null || acc.getLong(indexOfCountStar) == 0;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
