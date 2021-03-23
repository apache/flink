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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.WindowCombineFunction;
import org.apache.flink.table.runtime.operators.window.slicing.SliceUnsharedAssigner;

/**
 * An window aggregate processor implementation which works for {@link SliceUnsharedAssigner}, e.g.
 * tumbling windows.
 */
public final class SliceUnsharedWindowAggProcessor extends AbstractWindowAggProcessor {
    private static final long serialVersionUID = 1L;

    public SliceUnsharedWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            WindowBuffer.Factory windowBufferFactory,
            WindowCombineFunction.Factory combineFactory,
            SliceUnsharedAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer) {
        super(genAggsHandler, windowBufferFactory, combineFactory, sliceAssigner, accSerializer);
    }

    @Override
    public void fireWindow(Long windowEnd) throws Exception {
        RowData acc = windowState.value(windowEnd);
        if (acc == null) {
            acc = aggregator.createAccumulators();
        }
        aggregator.setAccumulators(windowEnd, acc);
        RowData aggResult = aggregator.getValue(windowEnd);
        collect(aggResult);
    }
}
