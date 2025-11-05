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

package org.apache.flink.table.runtime.operators.deduplicate.utils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Utility for deduplicate function.
 *
 * <p>TODO utilize the respective helper classes that inherit from an abstract deduplicate function
 * helper in each deduplicate function.
 */
public class DeduplicateFunctionHelper {

    /**
     * Processes element to deduplicate on keys with process time semantic, sends current element as
     * last row, retracts previous element if needed.
     *
     * @param currentRow latest row received by deduplicate function
     * @param generateUpdateBefore whether need to send UPDATE_BEFORE message for updates
     * @param state state of function, null if generateUpdateBefore is false
     * @param out underlying collector
     * @param isStateTtlEnabled whether state ttl is disabled
     * @param equaliser the record equaliser used to equal RowData.
     */
    public static void processLastRowOnProcTime(
            RowData currentRow,
            boolean generateUpdateBefore,
            boolean generateInsert,
            ValueState<RowData> state,
            Collector<RowData> out,
            boolean isStateTtlEnabled,
            RecordEqualiser equaliser)
            throws Exception {

        checkInsertOnly(currentRow);
        if (generateUpdateBefore || generateInsert) {
            // use state to keep the previous row content if we need to generate UPDATE_BEFORE
            // or use to distinguish the first row, if we need to generate INSERT
            RowData preRow = state.value();
            state.update(currentRow);
            if (preRow == null) {
                // the first row, send INSERT message
                currentRow.setRowKind(RowKind.INSERT);
                out.collect(currentRow);
            } else {
                if (!isStateTtlEnabled && equaliser.equals(preRow, currentRow)) {
                    // currentRow is the same as preRow and state cleaning is not enabled.
                    // We do not emit retraction and update message.
                    // If state cleaning is enabled, we have to emit messages to prevent too early
                    // state eviction of downstream operators.
                    return;
                } else {
                    if (generateUpdateBefore) {
                        preRow.setRowKind(RowKind.UPDATE_BEFORE);
                        out.collect(preRow);
                    }
                    currentRow.setRowKind(RowKind.UPDATE_AFTER);
                    out.collect(currentRow);
                }
            }
        } else {
            // always send UPDATE_AFTER if INSERT is not needed
            currentRow.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(currentRow);
        }
    }

    /**
     * Processes element to deduplicate on keys, sends current element as last row, retracts
     * previous element if needed.
     *
     * <p>Note: we don't support stateless mode yet. Because this is not safe for Kafka tombstone
     * messages which doesn't contain full content. This can be a future improvement if the
     * downstream (e.g. sink) doesn't require full content for DELETE messages.
     *
     * @param currentRow latest row received by deduplicate function
     * @param generateUpdateBefore whether need to send UPDATE_BEFORE message for updates
     * @param state state of function
     * @param out underlying collector
     */
    public static void processLastRowOnChangelog(
            RowData currentRow,
            boolean generateUpdateBefore,
            ValueState<RowData> state,
            Collector<RowData> out,
            boolean isStateTtlEnabled,
            RecordEqualiser equaliser)
            throws Exception {
        RowData preRow = state.value();
        RowKind currentKind = currentRow.getRowKind();
        if (currentKind == RowKind.INSERT || currentKind == RowKind.UPDATE_AFTER) {
            if (preRow == null) {
                // the first row, send INSERT message
                currentRow.setRowKind(RowKind.INSERT);
                out.collect(currentRow);
            } else {
                if (!isStateTtlEnabled && areRowsWithSameContent(equaliser, preRow, currentRow)) {
                    // currentRow is the same as preRow and state cleaning is not enabled.
                    // We do not emit retraction and update message.
                    // If state cleaning is enabled, we have to emit messages to prevent too early
                    // state eviction of downstream operators.
                    return;
                } else {
                    if (generateUpdateBefore) {
                        preRow.setRowKind(RowKind.UPDATE_BEFORE);
                        out.collect(preRow);
                    }
                    currentRow.setRowKind(RowKind.UPDATE_AFTER);
                    out.collect(currentRow);
                }
            }
            // normalize row kind
            currentRow.setRowKind(RowKind.INSERT);
            // save to state
            state.update(currentRow);
        } else {
            // DELETE or UPDATER_BEFORE
            if (preRow != null) {
                // always set to DELETE because this row has been removed
                // even the input is UPDATE_BEFORE, there may no UPDATE_AFTER after it.
                preRow.setRowKind(RowKind.DELETE);
                // output the preRow instead of currentRow,
                // because preRow always contains the full content.
                // currentRow may only contain key parts (e.g. Kafka tombstone records).
                out.collect(preRow);
                // clear state as the row has been removed
                state.clear();
            }
            // nothing to do if removing a non-existed row
        }
    }

    public static void processLastRowOnChangelogWithFilter(
            FilterCondition.Context context,
            RowData currentRow,
            boolean generateUpdateBefore,
            ValueState<RowData> state,
            Collector<RowData> out,
            boolean isStateTtlEnabled,
            RecordEqualiser equaliser,
            FilterCondition filterCondition)
            throws Exception {
        RowData preRow = state.value();
        RowKind currentKind = currentRow.getRowKind();
        if (currentKind == RowKind.INSERT || currentKind == RowKind.UPDATE_AFTER) {
            if (preRow == null) {
                if (filterCondition.apply(context, currentRow)) {
                    // the first row, send INSERT message
                    currentRow.setRowKind(RowKind.INSERT);
                    out.collect(currentRow);
                } else {
                    // return, do not update the state
                    return;
                }
            } else {
                if (!isStateTtlEnabled && areRowsWithSameContent(equaliser, preRow, currentRow)) {
                    // currentRow is the same as preRow and state cleaning is not enabled.
                    // We do not emit retraction and update message.
                    // If state cleaning is enabled, we have to emit messages to prevent too early
                    // state eviction of downstream operators.
                    return;
                } else {
                    if (filterCondition.apply(context, currentRow)) {
                        if (generateUpdateBefore) {
                            preRow.setRowKind(RowKind.UPDATE_BEFORE);
                            out.collect(preRow);
                        }
                        currentRow.setRowKind(RowKind.UPDATE_AFTER);
                        out.collect(currentRow);
                    } else {
                        // generate retraction, because the row does not match any longer
                        preRow.setRowKind(RowKind.DELETE);
                        out.collect(preRow);
                        // clear the state, there is no row we will need to retract
                        state.clear();
                        return;
                    }
                }
            }
            // normalize row kind
            currentRow.setRowKind(RowKind.INSERT);
            // save to state
            state.update(currentRow);
        } else {
            // DELETE or UPDATER_BEFORE
            if (preRow != null) {
                // always set to DELETE because this row has been removed
                // even the input is UPDATE_BEFORE, there may no UPDATE_AFTER after it.
                preRow.setRowKind(RowKind.DELETE);
                // output the preRow instead of currentRow,
                // because preRow always contains the full content.
                // currentRow may only contain key parts (e.g. Kafka tombstone records).
                out.collect(preRow);
                // clear state as the row has been removed
                state.clear();
            }
            // nothing to do if removing a non-existed row
        }
    }

    /**
     * Processes element to deduplicate on keys with process time semantic, sends current element if
     * it is first row.
     *
     * @param currentRow latest row received by deduplicate function
     * @param state state of function
     * @param out underlying collector
     */
    public static void processFirstRowOnProcTime(
            RowData currentRow, ValueState<Boolean> state, Collector<RowData> out)
            throws Exception {

        checkInsertOnly(currentRow);
        // ignore record if it is not first row
        if (state.value() != null) {
            return;
        }
        state.update(true);
        // emit the first row which is INSERT message
        out.collect(currentRow);
    }

    /**
     * Collect the updated result for duplicate row.
     *
     * @param generateUpdateBefore flag to generate UPDATE_BEFORE message or not
     * @param generateInsert flag to generate INSERT message or not
     * @param preRow previous row under the key
     * @param currentRow current row under the key which is the duplicate row
     * @param out underlying collector
     */
    public static void updateDeduplicateResult(
            boolean generateUpdateBefore,
            boolean generateInsert,
            RowData preRow,
            RowData currentRow,
            Collector<RowData> out) {

        if (generateUpdateBefore || generateInsert) {
            if (preRow == null) {
                // the first row, send INSERT message
                currentRow.setRowKind(RowKind.INSERT);
                out.collect(currentRow);
            } else {
                if (generateUpdateBefore) {
                    final RowKind preRowKind = preRow.getRowKind();
                    preRow.setRowKind(RowKind.UPDATE_BEFORE);
                    out.collect(preRow);
                    preRow.setRowKind(preRowKind);
                }
                currentRow.setRowKind(RowKind.UPDATE_AFTER);
                out.collect(currentRow);
            }
        } else {
            currentRow.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(currentRow);
        }
    }

    /** Returns true if currentRow should be kept. */
    public static boolean shouldKeepCurrentRow(
            RowData preRow, RowData currentRow, int rowtimeIndex, boolean keepLastRow) {
        if (keepLastRow) {
            return preRow == null
                    || getRowtime(preRow, rowtimeIndex) <= getRowtime(currentRow, rowtimeIndex);
        } else {
            return preRow == null
                    || getRowtime(currentRow, rowtimeIndex) < getRowtime(preRow, rowtimeIndex);
        }
    }

    /**
     * Important: the method assumes that {@code currentRow} comes either with {@code
     * RowKind.UPDATE_AFTER} or with {@code RowKind.INSERT}. It is not designed to be used for other
     * cases.
     */
    private static boolean areRowsWithSameContent(
            RecordEqualiser equaliser, RowData prevRow, RowData currentRow) {
        final RowKind currentRowKind = currentRow.getRowKind();
        if (currentRowKind == RowKind.UPDATE_AFTER) {
            // setting row kind to prevRowKind to check whether the row content is the same
            currentRow.setRowKind(RowKind.INSERT);
            final boolean result = equaliser.equals(prevRow, currentRow);
            currentRow.setRowKind(currentRowKind);
            return result;
        }
        return equaliser.equals(prevRow, currentRow);
    }

    private static long getRowtime(RowData input, int rowtimeIndex) {
        return input.getLong(rowtimeIndex);
    }

    /** check message should be insert only. */
    public static void checkInsertOnly(RowData currentRow) {
        Preconditions.checkArgument(currentRow.getRowKind() == RowKind.INSERT);
    }

    private DeduplicateFunctionHelper() {}
}
