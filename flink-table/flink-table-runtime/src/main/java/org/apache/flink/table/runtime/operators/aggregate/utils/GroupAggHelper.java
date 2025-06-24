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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.aggregate.RecordCounter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;

/** A helper to do the logic of group agg. */
public abstract class GroupAggHelper {

    /** Used to count the number of added and retracted input records. */
    private final RecordCounter recordCounter;

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;

    /** State idle retention config. */
    private final StateTtlConfig ttlConfig;

    /** function used to handle all aggregates. */
    private final AggsHandleFunction function;

    /** function used to equal RowData. */
    private final RecordEqualiser equaliser;

    /** Reused output row. */
    private final JoinedRowData resultRow;

    public GroupAggHelper(
            RecordCounter recordCounter,
            boolean generateUpdateBefore,
            StateTtlConfig ttlConfig,
            AggsHandleFunction function,
            RecordEqualiser equaliser) {
        this.recordCounter = recordCounter;
        this.generateUpdateBefore = generateUpdateBefore;
        this.ttlConfig = ttlConfig;
        this.function = function;
        this.equaliser = equaliser;
        this.resultRow = new JoinedRowData();
    }

    public void processElement(
            RowData input, RowData currentKey, RowData accumulators, Collector<RowData> out)
            throws Exception {
        boolean firstRow;
        if (null == accumulators) {
            // Don't create a new accumulator for a retraction message. This
            // might happen if the retraction message is the first message for the
            // key or after a state clean up.
            if (isRetractMsg(input)) {
                return;
            }
            firstRow = true;
            accumulators = function.createAccumulators();
        } else {
            firstRow = false;
        }

        // set accumulators to handler first
        function.setAccumulators(accumulators);
        // get previous aggregate result
        RowData prevAggValue = function.getValue();

        // update aggregate result and set to the newRow
        if (isAccumulateMsg(input)) {
            // accumulate input
            function.accumulate(input);
        } else {
            // retract input
            function.retract(input);
        }
        // get current aggregate result
        RowData newAggValue = function.getValue();

        // get accumulator
        accumulators = function.getAccumulators();

        if (!recordCounter.recordCountIsZero(accumulators)) {
            // we aggregated at least one record for this key

            // update the state
            updateAccumulatorsState(accumulators);

            // if this was not the first row and we have to emit retractions
            if (!firstRow) {
                if (!ttlConfig.isEnabled() && equaliser.equals(prevAggValue, newAggValue)) {
                    // newRow is the same as before and state cleaning is not enabled.
                    // We do not emit retraction and acc message.
                    // If state cleaning is enabled, we have to emit messages to prevent too early
                    // state eviction of downstream operators.
                    return;
                } else {
                    // retract previous result
                    if (generateUpdateBefore) {
                        // prepare UPDATE_BEFORE message for previous row
                        resultRow
                                .replace(currentKey, prevAggValue)
                                .setRowKind(RowKind.UPDATE_BEFORE);
                        out.collect(resultRow);
                    }
                    // prepare UPDATE_AFTER message for new row
                    resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
                }
            } else {
                // this is the first, output new result
                // prepare INSERT message for new row
                resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
            }

            out.collect(resultRow);

        } else {
            // we retracted the last record for this key
            // sent out a delete message
            if (!firstRow) {
                // prepare delete message for previous row
                resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
                out.collect(resultRow);
            }
            // and clear all state
            clearAccumulatorsState();
            // cleanup dataview under current key
            function.cleanup();
        }
    }

    protected abstract void updateAccumulatorsState(RowData accumulators) throws Exception;

    protected abstract void clearAccumulatorsState() throws Exception;
}
