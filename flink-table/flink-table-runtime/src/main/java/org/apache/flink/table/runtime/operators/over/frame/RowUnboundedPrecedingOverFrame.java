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
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;

/**
 * The row UnboundPreceding window frame calculates frames with the following SQL form: ... ROW
 * BETWEEN UNBOUNDED PRECEDING AND [window frame following] [window frame following] ::=
 * [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING.
 */
public class RowUnboundedPrecedingOverFrame extends UnboundedPrecedingOverFrame {

    private long rightBound;

    /** Index of the right bound input row. */
    private long inputRightIndex = 0;

    public RowUnboundedPrecedingOverFrame(
            GeneratedAggsHandleFunction aggsHandleFunction, long rightBound) {
        super(aggsHandleFunction);
        this.rightBound = rightBound;
    }

    @Override
    public void prepare(ResettableExternalBuffer rows) throws Exception {
        super.prepare(rows);
        inputRightIndex = 0;
    }

    @Override
    public RowData process(int index, RowData current) throws Exception {
        boolean bufferUpdated = index == 0;

        // Add all rows to the aggregates util right bound.
        while (nextRow != null && inputRightIndex <= index + rightBound) {
            processor.accumulate(nextRow);
            nextRow = OverWindowFrame.getNextOrNull(inputIterator);
            inputRightIndex += 1;
            bufferUpdated = true;
        }
        if (bufferUpdated) {
            accValue = processor.getValue();
        }
        return accValue;
    }
}
