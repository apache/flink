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

package org.apache.flink.table.runtime.operators.aggregate.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.TableAggsHandleFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;

/** A helper to do the logic of group table agg. */
public abstract class GroupTableAggHelper {

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;

    private final boolean incrementalUpdate;

    // function used to handle all table aggregates
    private transient TableAggsHandleFunction function = null;

    public GroupTableAggHelper(
            boolean generateUpdateBefore,
            boolean incrementalUpdate,
            TableAggsHandleFunction function) {
        this.generateUpdateBefore = generateUpdateBefore;
        this.incrementalUpdate = incrementalUpdate;
        this.function = function;
    }

    public RowData processElement(
            RowData accumulators, RowData currentKey, Collector<RowData> out, RowData input)
            throws Exception {
        boolean firstRow;
        if (null == accumulators) {
            firstRow = true;
            accumulators = function.createAccumulators();
        } else {
            firstRow = false;
        }

        // set accumulators to handler first
        function.setAccumulators(accumulators);

        // when incrementalUpdate is required, there is no need to retract
        // previous sent data which is not changed
        if (!firstRow && !incrementalUpdate && generateUpdateBefore) {
            function.emitValue(out, currentKey, true);
        }

        // update aggregate result and set to the newRow
        if (isAccumulateMsg(input)) {
            // accumulate input
            function.accumulate(input);
        } else {
            // retract input
            function.retract(input);
        }

        // get accumulator
        return function.getAccumulators();
    }
}
