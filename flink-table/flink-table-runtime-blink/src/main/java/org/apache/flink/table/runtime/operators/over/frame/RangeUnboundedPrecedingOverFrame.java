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

package org.apache.flink.table.runtime.operators.over.frame;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;

/**
 * The range UnboundPreceding window frame calculates frames with the following SQL form: ... RANGE
 * BETWEEN UNBOUNDED PRECEDING AND [window frame following] [window frame following] ::=
 * [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING.
 */
public class RangeUnboundedPrecedingOverFrame extends UnboundedPrecedingOverFrame {

    private GeneratedRecordComparator boundComparator;
    private RecordComparator rbound;

    public RangeUnboundedPrecedingOverFrame(
            GeneratedAggsHandleFunction aggsHandleFunction,
            GeneratedRecordComparator boundComparator) {
        super(aggsHandleFunction);
        this.boundComparator = boundComparator;
    }

    @Override
    public void open(ExecutionContext ctx) throws Exception {
        super.open(ctx);
        rbound = boundComparator.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        this.boundComparator = null;
    }

    @Override
    public RowData process(int index, RowData current) throws Exception {
        boolean bufferUpdated = index == 0;

        // Add all rows to the aggregates for which the input row value is equal to or less than
        // the output row upper bound.
        while (nextRow != null && rbound.compare(nextRow, current) <= 0) {
            processor.accumulate(nextRow);
            nextRow = OverWindowFrame.getNextOrNull(inputIterator);
            bufferUpdated = true;
        }
        if (bufferUpdated) {
            accValue = processor.getValue();
        }
        return accValue;
    }
}
