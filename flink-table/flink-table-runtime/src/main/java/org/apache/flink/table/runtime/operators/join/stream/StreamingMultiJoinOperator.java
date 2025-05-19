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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Streaming multi-way join operator which supports inner join and left outer join, right joins are
 * transformed into left joins by the optimizer. It only supports a combination of joins that joins
 * on at least one common column due to partitioning. It eliminates the intermediate state necessary
 * for a chain of multiple binary joins. In other words, it reduces the total amount of state
 * necessary for chained joins. As of time complexity, it performs better for the worst binary joins
 * cases, where the number of records in the intermediate state is large. Binary joins perform
 * better if they are optimally ordered, updates come mostly for the table on the right and the
 * query uses primary keys (the intermediate state for a specific join key is small).
 *
 * <p>Performs the multi-way join logic recursively. This method drives the join process by
 * traversing through the input streams (represented by `depth`) and their corresponding states. It
 * attempts to find matching combinations of rows across all inputs based on the defined join
 * conditions.
 *
 * <p><b>Core Idea:</b> The method explores a conceptual "join tree". Each level (`depth`)
 * corresponds to an input stream. At each level, it iterates through the records stored in the
 * state for that input. For each state record, it tentatively adds it to the `currentRows` array
 * and, if the relevant join condition passes ({@link #matchesCondition(int, RowData[])}),
 * recursively calls itself to process the next level (`depth + 1`). When the recursion reaches the
 * level corresponding to the triggering input record ({@link #isInputLevel(int, int)}), it
 * incorporates the `input` record itself into `currentRows` (again, subject to condition checks).
 * Finally, when the maximum depth is reached ({@link #isMaxDepth(int)}), it evaluates the final,
 * overall `multiJoinCondition` on the fully assembled `currentRows`.
 *
 * <p><b>Two-Mode Execution (controlled by `isInputRecordActive` flag):</b> The recursion operates
 * in two distinct modes, crucial for correctly handling LEFT joins:
 *
 * <ol>
 *   <li><b>Association Calculation Mode (`isInputRecordActive = false`):</b> This initial mode
 *       traverses the state. Its primary purpose is to iterate through records for each input.
 *       Also, for left joins, calculate the `associations` counts for the left side. This
 *       determines if rows from the "left" side found any matches on their respective "right" sides
 *       based on the {@link #joinConditions}. No results are emitted in this mode if the current
 *       input record is not yet active in `currentRows`. The recursion primarily stays in this mode
 *       until `processInputRecord` is invoked when the current depth matches the `inputId`, which
 *       then transitions to the result emission mode for its recursive calls.
 *   <li><b>Result Emission Mode (`isInputRecordActive = true`):</b> This mode is activated when
 *       `processInputRecord` processes the actual `input` record and makes subsequent recursive
 *       calls. These calls, now in result emission mode, incorporate the `input` record. When the
 *       recursion reaches the maximum depth (checked via {@link #isMaxDepth(int)}), it evaluates
 *       the final join conditions and emits the resulting joined row via the {@link #collector}.
 * </ol>
 *
 * <p><b>LEFT Join Specifics:</b> LEFT joins require special handling to ensure rows from the left
 * side are emitted even if they have no matching rows on the right side.
 *
 * <ul>
 *   <li><b>Condition Checks:</b>
 *       <ul>
 *         <li>At each step `d > 0`, the specific {@code joinConditions[d]} is evaluated using the
 *             rows accumulated so far (up to `currentRows[d]`). If this condition fails for a
 *             combination (from state or the input record), that recursive path is pruned via
 *             {@link #matchesCondition(int, RowData[])}.
 *         <li>At the maximum depth (base case), the final {@code multiJoinCondition} is evaluated
 *             on the complete `currentRows` array to determine if the overall joined row is valid.
 *       </ul>
 *   <li><b>Association Tracking ({@code associations} array):</b> {@code associations[d-1]} counts
 *       how many records from subsequent inputs (depth `d` onwards) have matched the current row at
 *       {@code currentRows[d-1]} based on the outer join conditions. This count is primarily
 *       updated when `isInputRecordActive` is `false` (during the Association Calculation Mode).
 *   <li><b>Null Padding:</b> If, after processing all state records for a LEFT join's right side
 *       (depth `d`), no matches were found (`!matched`) AND the corresponding left row also had no
 *       associations ({@link #hasNoAssociations(int, int[])}), it indicates the left row needs to
 *       be padded with nulls for the right side. This triggers {@link #processWithNullPadding(int,
 *       RowData, int, RowData[], int[], boolean)}, which places a null row at `currentRows[d]` and
 *       continues the recursion (passing the current `isInputRecordActive` value).
 *   <li><b>Input Record Handling (Upserts/Retractions):</b> When processing the actual `input`
 *       record at its native depth (`inputId`) in a LEFT join scenario:
 *       <ul>
 *         <li>If the input is an INSERT/UPDATE_AFTER and its preceding left-side row had no matches
 *             found when `isInputRecordActive` was `false` (during the association calculation
 *             pass, checked via {@link #hasNoAssociations(int, int[])}), a retraction (`DELETE`)
 *             may be emitted first for any previously padded result ({@link
 *             #handleRetractBeforeInput}). These operations occur in calls that will have
 *             `isInputRecordActive = true`.
 *         <li>If the input is a DELETE/UPDATE_BEFORE and its preceding left-side row had no
 *             matches, an insertion (`INSERT`) may be emitted for the new padded result (this also
 *             implicitly checks via {@link #hasNoAssociations(int, int[])} in the corresponding
 *             `if` condition in `processInputRecord`), ({@link #handleInsertAfterInput}). These
 *             operations occur in calls that will have `isInputRecordActive = true`.
 *       </ul>
 * </ul>
 *
 * <p><b>Base Case (Maximum Depth):</b> When {@link #isMaxDepth(int)} is true, all potential
 * contributing rows are in `currentRows`.
 *
 * <ul>
 *   <li>The final {@code multiJoinCondition} is evaluated on the complete `currentRows` array.
 *   <li>If the conditions pass and `isInputRecordActive` is `true` (Result Emission Mode), the
 *       combined row is constructed and emitted using {@link #emitRow(RowKind, RowData[])}. Note:
 *       if `isInputRecordActive` is `false`, rows might still be emitted if a full combination of
 *       state records forms a valid join.
 * </ul>
 *
 * <hr>
 *
 * <h3>Example Walkthrough (A LEFT JOIN B INNER JOIN C)</h3>
 *
 * <p>Inputs: A(idx=0), B(idx=1), C(idx=2)
 *
 * <p>Join: {@code A LEFT JOIN B ON A.id = B.id INNER JOIN C ON B.id = C.id}
 *
 * <p>Conditions:
 *
 * <ul>
 *   <li>{@code joinConditions[1]}: {@code A.id == B.id} (LEFT JOIN condition)
 *   <li>{@code joinConditions[2]}: {@code B.id == C.id} (INNER JOIN condition)
 *   <li>{@code multiJoinCondition}: {@code (A.id == B.id) && (B.id == C.id)} (Overall condition)
 * </ul>
 *
 * <p>Initial State:
 *
 * <ul>
 *   <li>StateA: {@code { a1(1, 100) }}
 *   <li>StateB: {@code { }}
 *   <li>StateC: {@code { c1(50, 501), c2(60, 601) }}
 * </ul>
 *
 * <p><b>=== Event 1: Input +b1(1, 50) arrives at Input B (inputId=1) ===</b>
 *
 * <pre><code>
 * Output: +I[a1(1,100), b1(1,50), c1(50,501)].
 * No INSERT for null padding emitted due to inner join with C. If this was
 * A LEFT JOIN B LEFT JOIN C instead of an inner join, we'd also retract this first -D[a1(1,100), NULL, NULL]).
 *
 * [Depth][currentRows]
 * [Depth 0][_, _, _] Initial Call: recursiveMultiJoin(0, +b1, 1, [_,_,_], [0,0,0], false) // isInputRecordActive = false
 * [Depth 0][_, _, _] Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 0][_, _, _]  Process StateA: { a1 }
 * [Depth 0][_, _, _]   Record a1:
 * [Depth 0][a1, _, _]     currentRows = [a1, _, _]
 * [Depth 0][a1, _, _]     isLeftJoin(0): false
 * [Depth 0][a1, _, _]     Recurse:
 * [Depth 1][a1, _, _]       Call: recursiveMultiJoin(1, +b1, 1, [a1,_,_], [0,0,0], false) // isInputRecordActive = false
 *
 * [Depth 1][a1, _, _]       Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 1][a1, _, _]       isLeftJoin(1): true (A LEFT B)
 * [Depth 1][a1, _, _]        Process StateB: {} -> Empty. 'matched' = false.
 * [Depth 1][a1, _, _] NULL_PAD? Check Null Padding: isLeftJoin(1) && !matched && hasNoAssociations(1, [0,0,0]) -> true
 * [Depth 1][a1, _, _] DO_NULL_PAD Call processWithNullPadding(1, +b1, 1, [a1,_,_], [0,0,0], false) // isInputRecordActive = false
 * [Depth 1][a1, nullB, _]     Set currentRows = [a1, nullB, _]
 * [Depth 1][a1, nullB, _]     Recurse to next depth:
 * [Depth 2][a1, nullB, _]       Call: recursiveMultiJoin(2, +b1, 1, [a1,nullB,_], [0,0,0], false) // isInputRecordActive = false
 *
 * [Depth 2][a1, nullB, _]       isLeftJoin(2): false
 * [Depth 2][a1, nullB, _]        Process StateC: { c1, c2 }
 * [Depth 2][a1, nullB, c1]        Record c1: currentRows = [a1, nullB, c1]. Check matchesCondition(2, [a1,nullB,c1]) -> fails (nullB.id != c1.id). Continue loop.
 * [Depth 2][a1, nullB, c2]        Record c2: currentRows = [a1, nullB, c2]. Check matchesCondition(2, [a1,nullB,c2]) -> fails (nullB.id != c2.id). Continue loop.
 * [Depth 2][a1, nullB, _]       StateC loop finishes. 'matched' = false.
 * [Depth 2][a1, nullB, _]       Return false.
 * [Depth 1][a1, _, _]         Return from processWithNullPadding: false. (Restores currentRows[1] to _ implicitly)
 * [Depth 1][a1, _, _]       'matched' from null padding is false.
 * [Depth 1][a1, _, _] INPUT_LVL? isInputLevel(1, 1): true -> Process the input record +b1 itself.
 * [Depth 1][a1, _, _] PROC_INPUT Call processInputRecord(1, +b1, 1, [a1,_,_], [0,0,0], false) -------> *** Mode switches to isInputRecordActive = true for subsequent recursive calls initiated by processInputRecord ***
 * [Depth 1][a1, _, _]           isLeftJoin(1): true
 * [Depth 1][a1, _, _] RETRACT?    Check Retract: isUpsert(+b1) && isLeftJoin(1) && !matched -> true && true && true -> true
 * [Depth 1][a1, _, _] DO_RETRACT  Call handleRetractBeforeInput(1, +b1, 1, [a1,_,_], [0,0,0])
 * [Depth 1][a1, nullB, _]         Set currentRows = [a1, nullB, _]
 * [Depth 1][a1, nullB, _]         input becomes temp -b1_temp
 * [Depth 1][a1, nullB, _]         Recurse:
 * [Depth 2][a1, nullB, _]           Call: recursiveMultiJoin(2, -b1_temp, 1, [a1,nullB,_], [0,0,0], true) // isInputRecordActive = true
 * [Depth 2][a1, nullB, _]           Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, nullB, _]            Process StateC: { c1, c2 }
 * [Depth 2][a1, nullB, c1]            Record c1: currentRows = [a1, nullB, c1]. Check matchesCondition(2, [a1,nullB,c1]) -> fails (nullB). Continue.
 * [Depth 2][a1, nullB, c2]            Record c2: currentRows = [a1, nullB, c2]. Check matchesCondition(2, [a1,nullB,c2]) -> fails (nullB). Continue.
 * [Depth 2][a1, nullB, _]           StateC loop returns false.
 * [Depth 2][a1, nullB, _]           Return false.
 * [Depth 1][a1, nullB, _]         handleRetractBeforeInput returns nothing. *** EMIT NOTHING, inner join does not match ***
 * [Depth 1][a1, +b1, _]         Restore input to +b1. Set currentRows = [a1, +b1, _].
 * [Depth 1][a1, +b1, _]         Check matchesCondition(1, [a1,+b1]) (a1.id == b1.id -> 1==1) -> true.
 * [Depth 1][a1, +b1, _] ASSOC_UPD   Update Associations: (current mode is isInputRecordActive=true, for input record +b1) associations[0]++. associations = [1, 0, 0].
 * [Depth 1][a1, +b1, _]         Recurse:
 * [Depth 2][a1, +b1, _]           Call: recursiveMultiJoin(2, +b1, 1, [a1,+b1,_], [1,0,0], true) // isInputRecordActive = true
 *
 * [Depth 2][a1, +b1, _]           Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, +b1, _]           isLeftJoin(2): false
 * [Depth 2][a1, +b1, _]            Process StateC: { c1, c2 }
 * [Depth 2][a1, +b1, c1]            Record c1: currentRows = [a1, +b1, c1]. Check matchesCondition(2, [a1,+b1,c1]) (b1.id == c1.id -> 50==50) -> true. Recurse:
 * [Depth 3][a1, +b1, c1]              Call: recursiveMultiJoin(3, +b1, 1, [a1,+b1,c1], [1,0,0], true) // isInputRecordActive = true
 * [Depth 3][a1, +b1, c1]              Mode: isInputRecordActive = true (Result Emission)
 * [Depth 3][a1, +b1, c1]              isMaxDepth(3): true
 * [Depth 3][a1, +b1, c1]              Evaluate multiJoinCondition([a1,+b1,c1]): (a1.id==b1.id && b1.id==c1.id) -> (1==1 && 50==50) -> true.
 * [Depth 3][a1, +b1, c1] *** EMIT ***  emitRow(INSERT, [a1, b1, c1]) // *** EMIT OUTPUT: +I[a1(1,100), b1(1,50), c1(50,501)] ***
 * [Depth 3][a1, +b1, c1]              Return true.
 * [Depth 2][a1, +b1, c2]            Record c2: currentRows = [a1, +b1, c2]. Check matchesCondition(2, [a1,+b1,c2]) (b1.id == c2.id -> 50==60) -> false. Continue loop.
 * [Depth 2][a1, +b1, _]           StateC loop returns true ('matched' = true because c1 matched).
 * [Depth 2][a1, +b1, _]           Return true.
 * [Depth 1][a1, +b1, _]         Return from processInputRecord: true.
 * [Depth 1][a1, +b1, _] INSERT?     Check Insert: isRetraction(+b1) is false. Skip handleInsertAfterInput.
 * [Depth 1][a1, +b1, _]         Return true.
 * [Depth 1][a1, _, _]     Return from Depth 1: true. (Restores currentRows[1] to _ implicitly)
 * [Depth 0][a1, _, _]   Return from Depth 0: true.
 * [Depth 0][_, _, _] End StateA loop. Return true. (Restores currentRows[0] to _ implicitly)
 *
 * --- End Event 1 ---
 * Add record to StateB: +b1(1, 50) -> StateB becomes { b1(1, 50) }.
 * StateB is now { b1(1, 50) }.
 * Output: +I[a1(1,100), b1(1,50), c1(50,501)].
 * No INSERT for null padding emitted due to inner join with C.
 * If this was A LEFT JOIN B LEFT JOIN C instead of a inner join, we'd have retracted this first -D[a1(1,100), NULL, NULL].
 * Note: The example shows detailed recursive calls. `recursiveMultiJoin` calls might return intermediate boolean `matched` values used internally, but the final output is the key outcome.
 * </code></pre>
 *
 * <p><b>=== Event 2: Input delete -b1(1, 50) arrives at Input B (inputId=1) ===</b> State
 *
 * <pre><code>
 * Before: StateB = { b1(1, 50) }
 * Output: -D[a1, b1, c1].
 * No INSERT for null padding emitted due to inner join with C.
 * If the query was A LEFT JOIN B LEFT JOIN C, we'd also emit a null padded row -I[a1(1,100), NULL, NULL].
 *
 * [Depth 0][_, _, _] Initial Call: recursiveMultiJoin(0, -b1, 1, [_,_,_], [0,0,0], false) // isInputRecordActive = false
 * [Depth 0][_, _, _] Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 0][_, _, _]  Process StateA: { a1 }
 * [Depth 0][_, _, _]   Record a1:
 * [Depth 0][a1, _, _]     currentRows = [a1, _, _]
 * [Depth 0][a1, _, _]     Recurse:
 * [Depth 1][a1, _, _]       Call: recursiveMultiJoin(1, -b1, 1, [a1,_,_], [0,0,0], false) // isInputRecordActive = false
 *
 * [Depth 1][a1, _, _]       Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 1][a1, _, _]       isLeftJoin(1): true
 * [Depth 1][a1, _, _]        Process StateB: { b1 } // State contains b1 from Event 1
 * [Depth 1][a1, b1, _]        Record b1: currentRows = [a1, b1, _]
 * [Depth 1][a1, b1, _]        Check matchesCondition(1, [a1, b1]) -> (a1.id == b1.id -> 1==1) -> true. Match found.
 * [Depth 1][a1, b1, _] ASSOC_UPD     Update Associations: (current mode is isInputRecordActive=false, for state record +b1) associations[0]++. associations = [1, 0, 0].
 * [Depth 1][a1, b1, _]          associations[1] = 0 // Reset for next level
 * [Depth 1][a1, b1, _]          Recurse:
 * [Depth 2][a1, b1, _]            Call: recursiveMultiJoin(2, -b1, 1, [a1, b1, _], [1, 0, 0], false) // isInputRecordActive = false
 * [Depth 2][a1, b1, _]            Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 2][a1, b1, _]            isLeftJoin(2): false
 * [Depth 2][a1, b1, _]             Process StateC: { c1, c2 }
 * [Depth 2][a1, b1, c1]              Record c1: currentRows = [a1, b1, c1]. Check matchesCondition(2, [a1,b1,c1]) -> (50==50) -> true. Recurse:
 * [Depth 3][a1, b1, c1]                Call: recursiveMultiJoin(3, -b1, 1, [a1,b1,c1], [1,0,0], false) // isInputRecordActive = false
 * [Depth 3][a1, b1, c1]                Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 3][a1, b1, c1]                isMaxDepth(3): true. Evaluate multiJoinCondition([a1,b1,c1]) -> (1==1 && 50==50) -> true. Return true. // Note: Emission occurs here even if isInputRecordActive=false
 * [Depth 2][a1, b1, c2]              Record c2: currentRows = [a1, b1, c2]. Check matchesCondition(2, [a1,b1,c2]) -> (50==60) -> false. Continue loop.
 * [Depth 2][a1, b1, _]            StateC loop returns true (c1 matched).
 * [Depth 2][a1, b1, _]            Return true.
 * [Depth 1][a1, b1, _]        StateB loop finishes. matched = true.
 * [Depth 1][a1, b1, _] NULL_PAD?    Check Null Padding: isLeftJoin(1) && !matched -> false. Skip null padding.
 * [Depth 1][a1, b1, _] INPUT_LVL?   isInputLevel(1, 1): true -> Process input record -b1.
 * [Depth 1][a1, _, _] PROC_INPUT   Call processInputRecord(1, -b1, 1, [a1,_,_], [1,0,0], true) -- Mode switches to isInputRecordActive = true for subsequent recursive calls initiated by processInputRecord
 * [Depth 1][a1, _, _]            isLeftJoin(1): true
 * [Depth 1][a1, _, _] RETRACT?     Check Retract: isUpsert(-b1) is false. Skip handleRetractBeforeInput.
 * [Depth 1][a1, -b1, _]         Set currentRows = [a1, -b1, _].
 * [Depth 1][a1, -b1, _]         Check matchesCondition(1, [a1,-b1]) (a1.id == b1.id -> 1==1) -> true. Match found.
 * [Depth 1][a1, -b1, _] ASSOC_UPD    Update Associations: (current mode is isInputRecordActive=true, for input record -b1) associations[0]--. associations = [0, 0, 0].
 * [Depth 1][a1, -b1, _]         Recurse:
 * [Depth 2][a1, -b1, _]           Call: recursiveMultiJoin(2, -b1, 1, [a1, -b1, _], [0, 0, 0], true) // isInputRecordActive = true
 * [Depth 2][a1, -b1, _]           Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, -b1, _]            Process StateC: { c1, c2 }
 * [Depth 2][a1, -b1, c1]            Record c1: currentRows = [a1, -b1, c1]. Check matchesCondition(2, [a1,-b1,c1]) -> (b1.id==c1.id -> 50==50) -> true. Recurse:
 * [Depth 3][a1, -b1, c1]              Call: recursiveMultiJoin(3, -b1, 1, [a1, -b1, c1], [0, 0, 0], true) // isInputRecordActive = true
 * [Depth 3][a1, -b1, c1]              Mode: isInputRecordActive = true (Result Emission)
 * [Depth 3][a1, -b1, c1]              isMaxDepth(3): true. Evaluate multiJoinCondition([a1,-b1,c1]) -> (1==1 && 50==50) -> true.
 * [Depth 3][a1, -b1, c1] *** EMIT *** emitRow(DELETE, [a1, b1, c1]) // *** EMIT OUTPUT: -D[a1(1,100), b1(1,50), c1(50,501)] ***
 * [Depth 3][a1, -b1, c1]              Return true.
 * [Depth 2][a1, -b1, c2]            Record c2: currentRows = [a1, -b1, c2]. Check matchesCondition(2, [a1,-b1,c2]) -> (b1.id==c2.id -> 50==60) -> false. Continue loop.
 * [Depth 2][a1, -b1, _]           StateC loop returns true (c1 matched).
 * [Depth 2][a1, -b1, _]           Return true. matched_input = true.
 * [Depth 1][a1, -b1, _] INSERT?      Check Insert: isRetraction(-b1) && isLeftJoin(1) && hasNoAssociations(1, [0,0,0]) -> true && true && true. -> true
 * [Depth 1][a1, -b1, _] DO_INSERT    Call handleInsertAfterInput(1, -b1, 1, [a1,-b1,_], [0,0,0]) -- EMIT NULL PADDING INSERT?
 * [Depth 1][a1, -b1, _]             // Attempts to emit the padded row [a1, nullB, ...] combined with state from C
 * [Depth 1][a1, nullB, _]           currentRows = [a1, nullB, _]
 * [Depth 1][a1, nullB, _]           input becomes temp +b1_temp (Kind.INSERT)
 * [Depth 1][a1, nullB, _]           Recurse:
 * [Depth 2][a1, nullB, _]             Call: recursiveMultiJoin(2, +b1_temp, 1, [a1, nullB, _], [0, 0, 0], true) // isInputRecordActive = true
 * [Depth 2][a1, nullB, _]             Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, nullB, _]             isLeftJoinAtDepth(2) is false (B INNER JOIN C).
 * [Depth 2][a1, nullB, _]              Process StateC: { c1, c2 }
 * [Depth 2][a1, nullB, c1]              Record c1: currentRows = [a1, nullB, c1]. Check matchesCondition(2, [a1,nullB,c1]) fails (nullB). Continue.
 * [Depth 2][a1, nullB, c2]              Record c2: currentRows = [a1, nullB, c2]. Check matchesCondition(2, [a1,nullB,c2]) fails (nullB). Continue.
 * [Depth 2][a1, nullB, _]             NULL_PAD? isLeftJoin && !matched && hasNoAssociations(depth, associations) -> not left join, false.
 * [Depth 2][a1, nullB, _]             INPUT_LVL? isInputLevel(depth, inputId) -> false
 * [Depth 2][a1, nullB, _]             *** EMIT NOTHING since the outer inner join does not match. ***
 * [Depth 2][a1, nullB, _]             StateC loop returns false.
 * [Depth 2][a1, nullB, _]             No call to processWithNullPadding as isLeftJoinAtDepth(2) is false.
 * [Depth 2][a1, nullB, _]             Return false.
 * [Depth 1][a1, nullB, _]           No row emitted because multiJoinCondition failed for all combinations with StateC.
 * [Depth 1][a1, -b1, _]           handleInsertAfterInput restores input kind (-b1), returns false. (Restores currentRows[1])
 * [Depth 1][a1, -b1, _]         processInputRecord returns true (because matched_input was true before handleInsertAfterInput).
 * [Depth 1][a1, _, _]       Return from Depth 1: true. (Restores currentRows[1] implicitly)
 * [Depth 0][a1, _, _]   Return from Depth 0: true.
 * [Depth 0][_, _, _] End StateA loop. Return true. (Restores currentRows[0] to _ implicitly)
 *
 * --- End Event 2 ---
 * Add record to StateB: -b1(1, 50) -> StateB becomes {}.
 * Output: -D[a1, b1, c1].
 * No INSERT for null padding emitted due to inner join with C.
 * </code></pre>
 */
public class StreamingMultiJoinOperator extends AbstractStreamOperatorV2<RowData>
        implements MultipleInputStreamOperator<RowData> {
    private static final long serialVersionUID = 1L;

    /** List of supported join types. */
    public enum JoinType {
        INNER,
        LEFT
    }

    private final List<JoinInputSideSpec> inputSpecs;
    private final List<JoinType> joinTypes;
    private final List<InternalTypeInfo<RowData>> inputTypes;
    // The multiJoinCondition is currently not being used, since we check the join conditions
    // for each while iterating through records to shortcircuit the recursion. However, if we
    // eventually want to cache join results at some level or do some other optimizations, this
    // might become useful.
    // TODO I'm not sure what's the best approach here: provide extra arguments so that we
    // avoid operator migrations or remove it since we don't need it for now.
    private final MultiJoinCondition multiJoinCondition;
    private final long[] stateRetentionTime;
    private final List<Input<RowData>> typedInputs;
    private final MultiJoinCondition[] joinConditions;
    private final JoinKeyExtractor keyExtractor;

    private transient List<MultiJoinStateView> stateHandlers;
    private transient TimestampedCollector<RowData> collector;
    private transient List<RowData> nullRows;

    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<InternalTypeInfo<RowData>> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<JoinType> joinTypes,
            MultiJoinCondition multiJoinCondition,
            long[] stateRetentionTime,
            MultiJoinCondition[] joinConditions,
            JoinKeyExtractor keyExtractor) {
        super(parameters, inputSpecs.size());
        this.inputTypes = inputTypes;
        this.inputSpecs = inputSpecs;
        this.joinTypes = joinTypes;
        this.multiJoinCondition = multiJoinCondition;
        this.stateRetentionTime = stateRetentionTime;
        this.joinConditions = joinConditions;
        this.keyExtractor = keyExtractor;
        this.typedInputs = new ArrayList<>(inputSpecs.size());
    }

    @Override
    public void open() throws Exception {
        super.open();
        initializeCollector();
        initializeNullRows();
        initializeStateHandlers();
    }

    @Override
    public void close() throws Exception {
        closeConditions();
        super.close();
    }

    public void processElement(int inputId, StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        if (input == null) {
            return;
        }

        performMultiJoin(input, inputId);
        addRecordToState(input, inputId);
    }

    private void performMultiJoin(RowData input, int inputId) throws Exception {
        int[] associations = createInitialAssociations();
        RowData[] currentRows = new RowData[inputSpecs.size()];

        recursiveMultiJoin(
                0,
                input,
                inputId,
                currentRows,
                associations,
                false); // isInputRecordActive = false initially
    }

    /**
     * See {@link StreamingMultiJoinOperator} for a detailed explanation of the recursive join and
     * examples.
     *
     * @param depth The current depth of the recursion, representing the input stream index (0 to
     *     N-1).
     * @param input The original input record that triggered this join operation.
     * @param inputId The index of the input stream from which the `input` record originated.
     * @param currentRows An array holding the candidate row from each input stream processed so far
     *     in this recursive path. `currentRows[d]` holds the row from input `d`.
     * @param associations An array used for LEFT joins to track match counts. `associations[d]`
     *     stores the number of successful matches found for `currentRows[d]` against inputs `d+1`
     *     onwards based on outer join conditions.
     * @param isInputRecordActive True when `currentRows` contains the new added input record at its
     *     `inputId` (via `processInputRecord`), and we are in the mode to emit matching join row
     *     combinations. False if we are in the initial state exploration/association calculation
     *     mode.
     * @throws Exception If state access or condition evaluation fails.
     */
    private void recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            boolean isInputRecordActive)
            throws Exception {
        // Base case: If we've processed all inputs and reached the last level, all join conditions
        // for each level have matched. We now emit the joined output record.
        if (isMaxDepth(depth)) {
            emitJoinedRow(input, currentRows);
            return;
        }
        boolean isLeftJoin = isLeftJoinAtDepth(depth);

        // Store a record from the current level in currentRows and go to the next index.
        // We iterate through records using a recursive depth first search approach.
        // We store matched here because we need to know if we need to do emit a null padded output
        // if there were no matching records.
        boolean matched =
                processRecords(
                        depth,
                        input,
                        inputId,
                        currentRows,
                        associations,
                        isInputRecordActive,
                        isLeftJoin);

        // If the current depth is the one where the triggering input record arrived,
        // now process the input record itself with the current combination of rows we are at.
        // processInputRecord will handle transitioning to the "emit results" mode
        // (isInputRecordActive =
        // true for its recursive calls).
        if (isInputLevel(depth, inputId)) {
            processInputRecord(depth, input, inputId, currentRows, associations, matched);
        } else if (isLeftJoin && !matched) {
            // For LEFT joins, if no matches were found in the state and the left side has no
            // associations, process with null padding for the current depth. In other words,
            // we emit null for this level. This is important so we continue to the join
            // with the output of this join level, which is a null padded row.
            // Continue with the same isInputRecordActive mode.
            processWithNullPadding(
                    depth, input, inputId, currentRows, associations, isInputRecordActive);
        }
    }

    // This simply emits the resulting join row between all n inputs.
    private void emitJoinedRow(RowData input, RowData[] currentRows) {
        emitRow(input.getRowKind(), currentRows);
    }

    // Problem: we are not using the primary key but rather always the partitioning key
    private boolean processRecords(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            boolean isInputRecordActive,
            boolean isLeftJoin)
            throws Exception {
        boolean matched = false; // Tracks if any record at this depth matched the condition

        // If an inner join and we reached the input level, we don't have to count the number of
        // associations that the left side has. This is only necessary for left joins to know
        // if we have to retract or insert null padded rows for the incoming record.
        // In other rows, we do not have to process any records in state for the inputId if
        // it's an inner join, since we do not need to know if we the left side has associations.
        if (isInnerJoin(isLeftJoin) && isInputLevel(depth, inputId)) {
            return false;
        }

        // Calculate the joinKey to retrieve from the state only the records that can potentially
        // match based on the equi-join conditions.
        // The joinKey consists of all attributes present in equi-join conditions for the current
        // level.
        // We use the left side (currentRows) to calculate its value.
        RowData joinKey = keyExtractor.getJoinKeyFromCurrentRows(depth, currentRows);
        Iterable<RowData> records = stateHandlers.get(depth).getRecords(joinKey);

        for (RowData record : records) {
            currentRows[depth] = record;

            // Shortcircuit: if the join condition fails, this path yields no results
            // we can go to the next record in the state.
            if (matchesCondition(depth, currentRows)) {
                matched = true;
            } else {
                continue;
            }

            // For LEFT joins, association counts are updated for the preceding level (depth - 1)
            // to correctly track if the left-side row found any matches on this right-side.
            // This information is crucial for determining if null padding is needed later.
            if (isLeftJoin) {
                updateAssociationCount(
                        depth,
                        associations,
                        shouldIncrementAssociation(isInputRecordActive, input));

                // Optimization: further recursion or counting might be skippable under
                // specific conditions detailed in `canOptimizeAssociationCounting`.
                if (canOptimizeAssociationCounting(depth, inputId, input, associations)) {
                    return true;
                }

                // The association count for the *current* depth is reset before recursing.
                // This is because `associations[depth]` should reflect matches found for the
                // current
                // combinations of records in `currentRows` with the matches it finds to the right.
                associations[depth] = 0;
            }

            // For the `inputId` level and when !isInputRecordActive (i.e., association calculation
            // mode),
            // the primary goal is to determine associations for the input record's level so we know
            // if we have to handle null padded retractions or insertions. Recursion to deeper
            // levels (joins to the right) is not needed for this specific count at this stage,
            // as the input record's participation and further joins are handled by
            // `processInputRecord` or subsequent recursive calls where isInputRecordActive will be
            // true.
            if (!isInputRecordActive && isInputLevel(depth, inputId)) {
                continue;
            }

            recursiveMultiJoin(
                    depth + 1, input, inputId, currentRows, associations, isInputRecordActive);
        }

        // Returns whether any record at this level matched the local condition.
        return matched;
    }

    private static boolean isInnerJoin(boolean isLeftJoin) {
        return !isLeftJoin;
    }

    private void processWithNullPadding(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            boolean isInputRecordActive)
            throws Exception {

        // Recursion continues with a null row at the current depth. This means the current join
        // emits a null padded output which is used for the next levels.
        // By continuing the recursion, we allow those deeper conditions to be evaluated and
        // ensures that the null padding correctly propagates to the join chain.
        currentRows[depth] = nullRows.get(depth);
        recursiveMultiJoin(
                depth + 1, input, inputId, currentRows, associations, isInputRecordActive);
    }

    private void processInputRecord(
            int depth,
            RowData input,
            int inputId,
            RowData[] currentRows,
            int[] associations,
            boolean matched)
            throws Exception {
        // if the join condition fails, this path yields no results.
        currentRows[depth] = input;
        if (!matchesCondition(depth, currentRows)) {
            return; // No match, nothing more to do on this path
        }

        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        // When processing the actual input record, if it satisfies the outer join condition
        // for this level (`depth`), its association with the preceding level (`depth-1`)
        // must be updated: either increment if upsert or decrement the number of associations, if a
        // retraction. This happens in the "result emission mode" (isInputRecordActive = true
        // implicitly for this logic path).
        if (isLeftJoin) {
            updateAssociationCount(
                    depth,
                    associations,
                    shouldIncrementAssociation(true, input)); // true for isInputRecordActive
        }

        // --- Left Join Retraction Handling ---
        // For an incoming INSERT/UPDATE_AFTER on the right side of a LEFT join,
        // if the corresponding left-side row previously had no matches (indicated by `!matched`
        // during
        // the association calculation phase where isInputRecordActive was false),
        // it would have resulted in a null-padded output. This new record might form a
        // valid join, so the previous null-padded output must be retracted.
        if (isUpsert(input) && isLeftJoin && !matched) {
            handleRetractBeforeInput(depth, input, inputId, currentRows, associations);
        }

        // Continue recursion to the next depth. Crucially, `isInputRecordActive` is now true
        // because we have incorporated the actual input record, and any further matches
        // found should lead to output generation or retractions.
        currentRows[depth] = input;
        recursiveMultiJoin(
                depth + 1,
                input,
                inputId,
                currentRows,
                associations,
                true); // true for isInputRecordActive

        // --- Left Join Insertion Handling ---
        // If an incoming DELETE/UPDATE_BEFORE on the right side of a LEFT join removes
        // the last matching record for the left-side row (checked via `hasNoAssociations`),
        // then according to LEFT join semantics, a new null-padded result must be inserted.
        if (isRetraction(input) && isLeftJoin && hasNoAssociations(depth, associations)) {
            handleInsertAfterInput(depth, input, inputId, currentRows, associations);
        }
    }

    private void handleRetractBeforeInput(
            int depth, RowData input, int inputId, RowData[] currentRows, int[] associations)
            throws Exception {
        // To construct the row that needs retraction, temporarily place a null row here.
        currentRows[depth] = nullRows.get(depth);
        RowKind originalKind = input.getRowKind();
        // Temporarily change RowKind to DELETE to trigger retraction downstream.
        input.setRowKind(RowKind.DELETE);

        // Recurse to emit the potential retraction for the previously null-padded row.
        // This is part of processing the input record, so isInputRecordActive = true.
        recursiveMultiJoin(
                depth + 1,
                input,
                inputId,
                currentRows,
                associations,
                true); // true for isInputRecordActive

        // Restore the input record's original RowKind to prevent unintended side effects,
        // as the `input` object itself was temporarily modified.
        input.setRowKind(originalKind);
    }

    private void handleInsertAfterInput(
            int depth, RowData input, int inputId, RowData[] currentRows, int[] associations)
            throws Exception {
        // To construct the new null-padded row, temporarily place a null row here.
        currentRows[depth] = nullRows.get(depth);
        RowKind originalKind = input.getRowKind();
        // Temporarily change RowKind to INSERT to trigger insertion downstream.
        input.setRowKind(RowKind.INSERT);

        // Recurse to emit the potential insertion for the new null-padded row.
        // This is part of processing the input record, so isInputRecordActive = true.
        recursiveMultiJoin(
                depth + 1,
                input,
                inputId,
                currentRows,
                associations,
                true); // true for isInputRecordActive

        // Restore the input record's original RowKind to prevent unintended side effects,
        // as the `input` object itself was temporarily modified.
        input.setRowKind(originalKind);
    }

    private void addRecordToState(RowData input, int inputId) throws Exception {
        RowData joinKey = keyExtractor.getJoinKeyFromInput(input, inputId);

        if (isRetraction(input)) {
            stateHandlers.get(inputId).retractRecord(joinKey, input);
        } else {
            stateHandlers.get(inputId).addRecord(joinKey, input);
        }
    }

    private void initializeCollector() {
        this.collector = new TimestampedCollector<>(output);
    }

    private void initializeNullRows() {
        this.nullRows = new ArrayList<>(inputTypes.size());
        for (InternalTypeInfo<RowData> inputType : inputTypes) {
            this.nullRows.add(new GenericRowData(inputType.toRowType().getFieldCount()));
        }
    }

    private void initializeStateHandlers() {
        if (this.stateHandler.getKeyedStateStore().isPresent()) {
            getRuntimeContext().setKeyedStateStore(this.stateHandler.getKeyedStateStore().get());
        } else {
            throw new RuntimeException(
                    "Keyed state store not found when initializing keyed state store handlers.");
        }

        this.stateHandlers = new ArrayList<>(inputSpecs.size());
        for (int i = 0; i < inputSpecs.size(); i++) {
            MultiJoinStateView stateView;
            String stateName = "multi-join-input-" + i;
            InternalTypeInfo<RowData> joinKeyType = keyExtractor.getJoinKeyType(i);

            if (joinKeyType == null) {
                throw new IllegalStateException(
                        "Could not determine joinKeyType for input "
                                + i
                                + ". State requires identifiable key attributes derived from join conditions.");
            }

            stateView =
                    MultiJoinStateViews.create(
                            getRuntimeContext(),
                            stateName,
                            inputSpecs.get(i),
                            joinKeyType,
                            inputTypes.get(i),
                            stateRetentionTime[i]);
            stateHandlers.add(stateView);
            typedInputs.add(createInput(i + 1));
        }
    }

    private void closeConditions() throws Exception {
        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }
    }

    private Input<RowData> createInput(int idx) {
        return new AbstractInput<>(this, idx) {
            @Override
            public void processElement(StreamRecord<RowData> element) throws Exception {
                ((StreamingMultiJoinOperator) owner)
                        .processElement(
                                idx - 1, // All internal logic is 0-based, so adjust the input ID.
                                element);
            }
        };
    }

    private void emitRow(RowKind rowKind, RowData[] rows) {
        RowData joinedRow = rows[0];
        for (int i = 1; i < rows.length; i++) {
            joinedRow = new JoinedRowData(rowKind, joinedRow, rows[i]);
        }
        collector.collect(joinedRow);
    }

    private boolean isUpsert(RowData row) {
        return row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER;
    }

    private boolean isRetraction(RowData row) {
        return row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
    }

    private boolean isLeftJoinAtDepth(int depth) {
        return depth > 0 && joinTypes.get(depth) == JoinType.LEFT;
    }

    /** Checks if the join condition specific to the current depth holds true. */
    private boolean matchesCondition(int depth, RowData[] currentRows) {
        // The first input (depth 0) doesn't have a preceding input to join with,
        // so there's no specific join condition to evaluate against `joinConditions[0]`.
        return depth == 0 || joinConditions[depth].apply(currentRows);
    }

    private void updateAssociationCount(int depth, int[] associations, boolean isUpsert) {
        if (isUpsert) {
            associations[depth - 1]++;
        } else {
            associations[depth - 1]--;
        }
    }

    private boolean shouldIncrementAssociation(boolean isInputRecordActive, RowData input) {
        // If not processing the active input record (i.e., in association calculation mode),
        // we always effectively "increment" for the purpose of counting potential matches from
        // state.
        // If processing the active input record, increment only for upserts.
        return !isInputRecordActive || isUpsert(input);
    }

    private int[] createInitialAssociations() {
        int[] associations = new int[inputSpecs.size()];
        Arrays.fill(associations, 0);
        return associations;
    }

    @SuppressWarnings({"rawtypes"})
    @Override
    public List<Input> getInputs() {
        // Instead of a direct cast from List<Input<RowData>> to List<Input> (which fails due to
        // Java's generics type erasure and invariance for collections),
        // we must create a new List<Input> and add elements. This is safe because
        // Input<RowData> is a subtype of the raw Input type.
        @SuppressWarnings({"rawtypes"})
        List<Input> rawInputs = new ArrayList<>(typedInputs.size());
        rawInputs.addAll(typedInputs);
        return rawInputs;
    }

    private boolean isMaxDepth(int depth) {
        return depth == inputSpecs.size();
    }

    private boolean isInputLevel(int depth, int inputId) {
        return depth == inputId;
    }

    private boolean hasNoAssociations(int depth, int[] associations) {
        return depth > 0 && associations[depth - 1] == 0;
    }

    /**
     * Optimization for LEFT joins at the input level.
     *
     * <p>We only need to know if *any* match exists (for upserts) or if *more than one* match
     * exists (for retractions) to correctly handle null padding logic. Counting beyond this minimum
     * requirement is unnecessary when `isInputRecordActive` is false.
     *
     * <p>Note: If further optimizations involving caching exact association counts are added, this
     * optimization might need to be removed.
     */
    private boolean canOptimizeAssociationCounting(
            int depth, int inputId, RowData input, int[] associations) {
        // This optimization is only relevant at the specific depth of the current input record
        // and not for the initial input (depth 0, which has no preceding associations).
        if (depth == 0 || inputId != depth) {
            return false;
        }

        if (isUpsert(input)) {
            // For an upsert, if at least one match is found (associations > 0),
            // we know a retraction of a prior null-padded row (if any) won't be necessary
            // before emitting the new joined row. Further counting adds no value here
            // during the association calculation phase (when isInputRecordActive is false).
            return associations[depth - 1] > 0;
        } else {
            // For a retraction, if more than one match existed (associations > 1),
            // removing this one input record means other matches still exist.
            // Therefore, we won't need to insert a new null-padded row after this retraction.
            // If associations[depth-1] was 1, then after decrementing it becomes 0,
            // and we would need to insert a null padded row.
            return associations[depth - 1] > 1;
        }
    }
}
