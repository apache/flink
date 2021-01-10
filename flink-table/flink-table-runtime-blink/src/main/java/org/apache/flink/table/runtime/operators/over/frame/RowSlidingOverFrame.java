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
import org.apache.flink.table.types.logical.RowType;

/**
 * The row sliding window frame calculates frames with the following SQL form: ... ROW BETWEEN
 * [window frame preceding] AND [window frame following] [window frame preceding] ::=
 * [unsigned_value_specification] PRECEDING | CURRENT ROW [window frame following] ::=
 * [unsigned_value_specification] FOLLOWING | CURRENT ROW
 *
 * <p>e.g.: ... ROW BETWEEN 1 PRECEDING AND 1 FOLLOWING.
 */
public class RowSlidingOverFrame extends SlidingOverFrame {

    private final long leftBound;
    private final long rightBound;

    /** Index of the right bound input row. */
    private int inputRightIndex = 0;

    /** Index of the left bound input row. */
    private int inputLeftIndex = 0;

    public RowSlidingOverFrame(
            RowType inputType,
            RowType valueType,
            GeneratedAggsHandleFunction aggsHandleFunction,
            long leftBound,
            long rightBound) {
        super(inputType, valueType, aggsHandleFunction);
        this.leftBound = leftBound;
        this.rightBound = rightBound;
    }

    @Override
    public void prepare(ResettableExternalBuffer rows) throws Exception {
        super.prepare(rows);
        inputRightIndex = 0;
        inputLeftIndex = 0;
    }

    @Override
    public RowData process(int index, RowData current) throws Exception {
        boolean bufferUpdated = index == 0;

        // Drop all rows from the buffer util left bound.
        while (!buffer.isEmpty() && inputLeftIndex < index + leftBound) {
            buffer.remove();
            inputLeftIndex += 1;
            bufferUpdated = true;
        }

        // Add all rows to the buffer util right bound.
        while (nextRow != null && inputRightIndex <= index + rightBound) {
            if (inputLeftIndex < index + leftBound) {
                inputLeftIndex += 1;
            } else {
                buffer.add(inputSer.copy(nextRow));
                bufferUpdated = true;
            }
            nextRow = OverWindowFrame.getNextOrNull(inputIterator);
            inputRightIndex += 1;
        }

        return accumulateBuffer(bufferUpdated);
    }
}
