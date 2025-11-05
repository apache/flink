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

import org.apache.flink.api.common.functions.DefaultOpenContext;
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
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.MultiJoinCondition;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.MultiJoinStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 * state for that input. For each state record, it tentatively adds it to the `joinedRowData` and,
 * if the relevant join condition passes ({@link #matchesCondition(int, RowData, RowData)}),
 * recursively calls itself to process the next level (`depth + 1`). When the recursion reaches the
 * level corresponding to the triggering input record ({@link #isInputLevel(int, int)}), it
 * incorporates the `input` record itself into `joinedRowData` (again, subject to condition checks).
 * Finally, when the maximum depth is reached ({@link #isMaxDepth(int)}), it evaluates the final,
 * overall `multiJoinCondition` on the fully assembled `joinedRowData`.
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
 *       input record is not yet active in `joinedRowData`. The recursion primarily stays in this
 *       mode until `processInputRecord` is invoked when the current depth matches the `inputId`,
 *       which then transitions to the result emission mode for its recursive calls.
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
 *             rows accumulated so far in `joinedRowData`. If this condition fails for a combination
 *             (from state or the input record), that recursive path is pruned via {@link
 *             #matchesCondition(int, RowData, RowData)}.
 *         <li>At the maximum depth (base case), the final {@code multiJoinCondition} is evaluated
 *             on the complete `joinedRowData` to determine if the overall joined row is valid.
 *       </ul>
 *   <li><b>Association Tracking:</b> Within the processing of records for a given `depth` (e.g., in
 *       {@link #processRecords} or {@link #processInputRecord}), an association count is maintained
 *       for the row at depth-1 in `joinedRowData` against the records or input being processed at
 *       depth. This count determines if the left-side row found matches on its right-side based on
 *       the outer join conditions. It is used, for example, in {@link #hasNoAssociations(int, int)}
 *       checks to determine if null padding is needed.
 *   <li><b>Null Padding:</b> If, after processing all state records for a LEFT join's right side
 *       (depth `d`), no matches were found (`!anyMatches` in {@code recursiveMultiJoin} derived
 *       from a `null` return of {@code processRecords}) AND the corresponding left row also had no
 *       associations based on the state-derived count ({@link #hasNoAssociations(int, int)}), it
 *       indicates the left row needs to be padded with nulls for the right side. This triggers
 *       {@link #processWithNullPadding(int, RowData, int, RowData, boolean)}, which places a null
 *       row at depth in `joinedRowData` and continues the recursion.
 *   <li><b>Input Record Handling (Upserts/Retractions):</b> When processing the actual `input`
 *       record at its native depth (`inputId`) in a LEFT join scenario:
 *       <ul>
 *         <li>If the input is an INSERT/UPDATE_AFTER and its preceding left-side row had no matches
 *             found when `isInputRecordActive` was `false` (during the association calculation
 *             pass, indicated by `!anyMatch` passed to {@code processInputRecord}), a retraction
 *             (`DELETE`) may be emitted first for any previously padded result ({@link
 *             #handleRetractBeforeInput}). These operations occur in calls that will have
 *             `isInputRecordActive = true`.
 *         <li>If the input is a DELETE/UPDATE_BEFORE and its preceding left-side row, after
 *             considering this input, ends up with no matches (checked via {@link
 *             #hasNoAssociations(int, int)} using the total association count in {@code
 *             processInputRecord}), an insertion (`INSERT`) may be emitted for the new padded
 *             result (this also implicitly checks via {@link #hasNoAssociations(int, int)} in the
 *             corresponding `if` condition in `processInputRecord`), ({@link
 *             #handleInsertAfterInput}). These operations occur in calls that will have
 *             `isInputRecordActive = true`.
 *       </ul>
 * </ul>
 *
 * <p><b>Base Case (Maximum Depth):</b> When {@link #isMaxDepth(int)} is true, all potential
 * contributing rows are in `joinedRowData`. We then emit final row with {@link
 * #emitJoinedRow(RowData, RowData)}.
 *
 * <p><hr>
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
 * [Depth][joinedRowData]
 * [Depth 0][_, _, _] Initial Call: recursiveMultiJoin(0, +b1, 1, [_,_,_], false) // isInputRecordActive = false
 * [Depth 0][_, _, _] Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 0][_, _, _]  Process StateA: { a1 }
 * [Depth 0][_, _, _]   Record a1:
 * [Depth 0][a1, _, _]     joinedRowData = [a1, _, _]
 * [Depth 0][a1, _, _]     isLeftJoin(0): false
 * [Depth 0][a1, _, _]     Call processRecords(0, +b1, 1, [a1,_,_], false, false) -> returns null (anyMatch=false, associationsPrevLevel=0)
 * [Depth 0][a1, _, _]     Recurse:
 * [Depth 1][a1, _, _]       Call: recursiveMultiJoin(1, +b1, 1, [a1,_,_], false) // isInputRecordActive = false
 *
 * [Depth 1][a1, _, _]       Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 1][a1, _, _]       isLeftJoin(1): true (A LEFT B)
 * [Depth 1][a1, _, _]        Call processRecords(1, +b1, 1, [a1,_,_], false, true) for StateB: {} -> returns null (anyMatch=false, associationsToPrevLevel for a1 = 0)
 * [Depth 1][a1, _, _]        anyMatches for a1 with StateB = false; associationsPrevLevel for a1 = 0.
 * [Depth 1][a1, _, _] NULL_PAD? Check Null Padding: isLeftJoin(1) && !anyMatches && hasNoAssociations(1, associationsPrevLevel=0) -> true
 * [Depth 1][a1, _, _] DO_NULL_PAD Call processWithNullPadding(1, +b1, 1, [a1,_,_], false) // isInputRecordActive = false
 * [Depth 1][a1, nullB, _]     Set joinedRowData = [a1, nullB, _]
 * [Depth 1][a1, nullB, _]     Recurse to next depth:
 * [Depth 2][a1, nullB, _]       Call: recursiveMultiJoin(2, +b1, 1, [a1,nullB,_], false) // isInputRecordActive = false
 *
 * [Depth 2][a1, nullB, _]       isLeftJoin(2): false
 * [Depth 2][a1, nullB, _]        Call processRecords(2, +b1, 1, [a1,nullB,_], false, false) for StateC: { c1, c2 }
 * [Depth 2][a1, nullB, c1]          Record c1: joinedRowData = [a1, nullB, c1]. Check matchesCondition(2, [a1,nullB,c1]) -> fails (nullB.id != c1.id). Continue loop.
 * [Depth 2][a1, nullB, c2]          Record c2: joinedRowData = [a1, nullB, c2]. Check matchesCondition(2, [a1,nullB,c2]) -> fails (nullB.id != c2.id). Continue loop.
 * [Depth 2][a1, nullB, _]        processRecords returns null (anyMatch=false, associations for nullB = 0)
 * [Depth 2][a1, nullB, _]       anyMatches for nullB with StateC = false; associationsPrevLevel for nullB = 0.
 * [Depth 2][a1, nullB, _]       Return (implicitly, no further processing for this path in processWithNullPadding's recursive call).
 * [Depth 1][a1, _, _]         Return from processWithNullPadding. (Restores joinedRowData[1] to _ implicitly)
 * [Depth 1][a1, _, _]       INPUT_LVL? isInputLevel(1, 1): true -> Process the input record +b1 itself.
 * [Depth 1][a1, _, _] PROC_INPUT Call processInputRecord(1, +b1, 1, [a1,_,_], associationsToPrevLevel=0, anyMatch=false) -------> *** Mode switches to isInputRecordActive = true for subsequent recursive calls initiated by processInputRecord ***
 * [Depth 1][a1, _, _]           isLeftJoin(1): true
 * [Depth 1][a1, _, _] RETRACT?    Check Retract: isUpsert(+b1) && isLeftJoin(1) && !anyMatch (is !false -> true) -> true
 * [Depth 1][a1, _, _] DO_RETRACT  Call handleRetractBeforeInput(1, +b1, 1, [a1,_,_])
 * [Depth 1][a1, nullB, _]         Set joinedRowData = [a1, nullB, _]
 * [Depth 1][a1, nullB, _]         input becomes temp -b1_temp
 * [Depth 1][a1, nullB, _]         Recurse:
 * [Depth 2][a1, nullB, _]           Call: recursiveMultiJoin(2, -b1_temp, 1, [a1,nullB,_], true) // isInputRecordActive = true
 * [Depth 2][a1, nullB, _]           Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, nullB, _]            Call processRecords(2, -b1_temp, 1, [a1,nullB,_], true, false) for StateC: { c1, c2 } -> returns null
 * [Depth 2][a1, nullB, _]           anyMatches for nullB with StateC = false.
 * [Depth 2][a1, nullB, _]           Return.
 * [Depth 1][a1, nullB, _]         handleRetractBeforeInput returns nothing. *** EMIT NOTHING, inner join does not match ***
 * [Depth 1][a1, +b1, _]         Restore input to +b1. Set joinedRowData = [a1, +b1, _].
 * [Depth 1][a1, +b1, _]         Check matchesCondition(1, [a1,+b1]) (a1.id == b1.id -> 1==1) -> true.
 * [Depth 1][a1, +b1, _] ASSOC_UPD   Update Associations: In processInputRecord, local 'associations' for a1 becomes 0 + 1 = 1.
 * [Depth 1][a1, +b1, _]         Recurse:
 * [Depth 2][a1, +b1, _]           Call: recursiveMultiJoin(2, +b1, 1, [a1,+b1,_], true) // isInputRecordActive = true
 *
 * [Depth 2][a1, +b1, _]           Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, +b1, _]           isLeftJoin(2): false
 * [Depth 2][a1, +b1, _]            Call processRecords(2, +b1, 1, [a1,+b1,_], true, false) for StateC: { c1, c2 }
 * [Depth 2][a1, +b1, c1]            Record c1: joinedRowData = [a1, +b1, c1]. Check matchesCondition(2, [a1,+b1,c1]) (b1.id == c1.id -> 50==50) -> true.
 * [Depth 2][a1, +b1, c1]            processRecords sees a match, local 'associations' for b1 becomes 1 (incremented if it were left). Recurses:
 * [Depth 3][a1, +b1, c1]              Call: recursiveMultiJoin(3, +b1, 1, [a1,+b1,c1], true) // isInputRecordActive = true
 * [Depth 3][a1, +b1, c1]              Mode: isInputRecordActive = true (Result Emission)
 * [Depth 3][a1, +b1, c1]              isMaxDepth(3): true
 * [Depth 3][a1, +b1, c1]              Evaluate multiJoinCondition([a1,+b1,c1]): (a1.id==b1.id && b1.id==c1.id) -> (1==1 && 50==50) -> true.
 * [Depth 3][a1, +b1, c1] *** EMIT ***  emitRow(INSERT, [a1, b1, c1]) // *** EMIT OUTPUT: +I[a1(1,100), b1(1,50), c1(50,501)] ***
 * [Depth 3][a1, +b1, c1]              Return.
 * [Depth 2][a1, +b1, c2]            Record c2: joinedRowData = [a1, +b1, c2]. Check matchesCondition(2, [a1,+b1,c2]) (b1.id == c2.id -> 50==60) -> false. Continue loop.
 * [Depth 2][a1, +b1, _]           processRecords for StateC returns Integer(0) (or 1 if join was left) - anyMatch=true, associations for b1 = 0 (or 1).
 * [Depth 2][a1, +b1, _]           anyMatches for b1 with StateC = true; associationsPrevLevel for b1 = 0 (or 1).
 * [Depth 2][a1, +b1, _]           Return.
 * [Depth 1][a1, +b1, _]         Return from processInputRecord.
 * [Depth 1][a1, +b1, _] INSERT?     Check Insert: isRetraction(+b1) is false. Skip handleInsertAfterInput.
 * [Depth 1][a1, +b1, _]         Return. (from recursiveMultiJoin depth 1)
 * [Depth 1][a1, _, _]     Return. (from recursiveMultiJoin depth 0)
 * [Depth 0][a1, _, _]   End StateA loop.
 * [Depth 0][_, _, _] End.
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
 * [Depth 0][_, _, _] Initial Call: recursiveMultiJoin(0, -b1, 1, [_,_,_], false) // isInputRecordActive = false
 * [Depth 0][_, _, _] Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 0][_, _, _]  Process StateA: { a1 }
 * [Depth 0][_, _, _]   Record a1:
 * [Depth 0][a1, _, _]     joinedRowData = [a1, _, _]
 * [Depth 0][a1, _, _]     Call processRecords(0, -b1, 1, [a1,_,_], false, false) -> returns null
 * [Depth 0][a1, _, _]     Recurse:
 * [Depth 1][a1, _, _]       Call: recursiveMultiJoin(1, -b1, 1, [a1,_,_], false) // isInputRecordActive = false
 *
 * [Depth 1][a1, _, _]       Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 1][a1, _, _]       isLeftJoin(1): true
 * [Depth 1][a1, _, _]        Call processRecords(1, -b1, 1, [a1,_,_], false, true) for StateB: { b1 }
 * [Depth 1][a1, b1, _]        Record b1: joinedRowData = [a1, b1, _]
 * [Depth 1][a1, b1, _]        Check matchesCondition(1, [a1, b1]) -> (a1.id == b1.id -> 1==1) -> true. Match found.
 * [Depth 1][a1, b1, _] ASSOC_UPD     Update Associations: In processRecords, local 'associations' for a1 becomes 1.
 * [Depth 1][a1, b1, _]          Recurse:
 * [Depth 2][a1, b1, _]            Call: recursiveMultiJoin(2, -b1, 1, [a1, b1, _], false) // isInputRecordActive = false
 * [Depth 2][a1, b1, _]            Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 2][a1, b1, _]            isLeftJoin(2): false
 * [Depth 2][a1, b1, _]             Call processRecords(2, -b1, 1, [a1,b1,_], false, false) for StateC: { c1, c2 }
 * [Depth 2][a1, b1, c1]              Record c1: joinedRowData = [a1, b1, c1]. Check matchesCondition(2, [a1,b1,c1]) -> (50==50) -> true. Recurse:
 * [Depth 3][a1, b1, c1]                Call: recursiveMultiJoin(3, -b1, 1, [a1,b1,c1], false) // isInputRecordActive = false
 * [Depth 3][a1, b1, c1]                Mode: isInputRecordActive = false (Association Calculation)
 * [Depth 3][a1, b1, c1]                isMaxDepth(3): true. Evaluate multiJoinCondition([a1,b1,c1]) -> (1==1 && 50==50) -> true. Return.
 * [Depth 2][a1, b1, c2]              Record c2: joinedRowData = [a1, b1, c2]. Check matchesCondition(2, [a1,b1,c2]) -> (50==60) -> false. Continue loop.
 * [Depth 2][a1, b1, _]            processRecords for StateC returns Integer(0) (or 1 if left). anyMatch=true.
 * [Depth 2][a1, b1, _]            Return.
 * [Depth 1][a1, b1, _]        processRecords for StateB returns Integer(1). anyMatch=true. (associations for a1 = 1)
 * [Depth 1][a1, b1, _]       anyMatches for a1 with StateB = true; associationsPrevLevel for a1 = 1.
 * [Depth 1][a1, b1, _] NULL_PAD?    Check Null Padding: isLeftJoin(1) && !anyMatches -> false. Skip null padding.
 * [Depth 1][a1, b1, _] INPUT_LVL?   isInputLevel(1, 1): true -> Process input record -b1.
 * [Depth 1][a1, _, _] PROC_INPUT   Call processInputRecord(1, -b1, 1, [a1,_,_], associationsToPrevLevel=1, anyMatch=true) -- Mode switches to isInputRecordActive = true for subsequent recursive calls initiated by processInputRecord
 * [Depth 1][a1, _, _]            isLeftJoin(1): true
 * [Depth 1][a1, _, _] RETRACT?     Check Retract: isUpsert(-b1) is false. Skip handleRetractBeforeInput.
 * [Depth 1][a1, -b1, _]         Set joinedRowData = [a1, -b1, _].
 * [Depth 1][a1, -b1, _]         Check matchesCondition(1, [a1,-b1]) (a1.id == b1.id -> 1==1) -> true. Match found.
 * [Depth 1][a1, -b1, _] ASSOC_UPD    Update Associations: In processInputRecord, local 'associations' for a1 becomes 1 - 1 = 0.
 * [Depth 1][a1, -b1, _]         Recurse:
 * [Depth 2][a1, -b1, _]           Call: recursiveMultiJoin(2, -b1, 1, [a1, -b1, _], true) // isInputRecordActive = true
 * [Depth 2][a1, -b1, _]           Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, -b1, _]            Call processRecords(2, -b1, 1, [a1,-b1,_], true, false) for StateC: { c1, c2 }
 * [Depth 2][a1, -b1, c1]            Record c1: joinedRowData = [a1, -b1, c1]. Check matchesCondition(2, [a1,-b1,c1]) -> (b1.id==c1.id -> 50==50) -> true. Recurse:
 * [Depth 3][a1, -b1, c1]              Call: recursiveMultiJoin(3, -b1, 1, [a1, -b1, c1], true) // isInputRecordActive = true
 * [Depth 3][a1, -b1, c1]              Mode: isInputRecordActive = true (Result Emission)
 * [Depth 3][a1, -b1, c1]              isMaxDepth(3): true. Evaluate multiJoinCondition([a1,-b1,c1]) -> (1==1 && 50==50) -> true.
 * [Depth 3][a1, -b1, c1] *** EMIT *** emitRow(DELETE, [a1, b1, c1]) // *** EMIT OUTPUT: -D[a1(1,100), b1(1,50), c1(50,501)] ***
 * [Depth 3][a1, -b1, c1]              Return.
 * [Depth 2][a1, -b1, c2]            Record c2: joinedRowData = [a1, -b1, c2]. Check matchesCondition(2, [a1,-b1,c2]) -> (b1.id==c2.id -> 50==60) -> false. Continue loop.
 * [Depth 2][a1, -b1, _]           processRecords for StateC returns Integer(0) (or 1 if left). anyMatch=true.
 * [Depth 2][a1, -b1, _]           Return.
 * [Depth 1][a1, -b1, _] INSERT?      Check Insert: isRetraction(-b1) && isLeftJoin(1) && hasNoAssociations(1, associations=0) -> true && true && true. -> true
 * [Depth 1][a1, -b1, _] DO_INSERT    Call handleInsertAfterInput(1, -b1, 1, [a1,-b1,_]) -- EMIT NULL PADDING INSERT?
 * [Depth 1][a1, -b1, _]             // Attempts to emit the padded row [a1, nullB, ...] combined with state from C
 * [Depth 1][a1, nullB, _]           joinedRowData = [a1, nullB, _]
 * [Depth 1][a1, nullB, _]           input becomes temp +b1_temp (Kind.INSERT)
 * [Depth 1][a1, nullB, _]           Recurse:
 * [Depth 2][a1, nullB, _]             Call: recursiveMultiJoin(2, +b1_temp, 1, [a1, nullB, _], true) // isInputRecordActive = true
 * [Depth 2][a1, nullB, _]             Mode: isInputRecordActive = true (Result Emission)
 * [Depth 2][a1, nullB, _]             isLeftJoinAtDepth(2) is false (B INNER JOIN C).
 * [Depth 2][a1, nullB, _]              Call processRecords(2, +b1_temp, 1, [a1,nullB,_], true, false) for StateC: { c1, c2 } -> returns null
 * [Depth 2][a1, nullB, _]             NULL_PAD? isLeftJoin && !anyMatches && hasNoAssociations(depth, associationsPrevLevel) -> not left join, false.
 * [Depth 2][a1, nullB, _]             INPUT_LVL? isInputLevel(depth, inputId) -> false
 * [Depth 2][a1, nullB, _]             *** EMIT NOTHING since the outer inner join does not match. ***
 * [Depth 2][a1, nullB, _]             Return.
 * [Depth 1][a1, nullB, _]           No row emitted because multiJoinCondition failed for all combinations with StateC.
 * [Depth 1][a1, -b1, _]           handleInsertAfterInput restores input kind (-b1).
 * [Depth 1][a1, -b1, _]         Return from processInputRecord.
 * [Depth 1][a1, _, _]       Return.
 * [Depth 0][a1, _, _]   Return.
 * [Depth 0][_, _, _] End.
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

    private final List<JoinInputSideSpec> inputSpecs;
    private final List<FlinkJoinType> joinTypes;
    private final List<RowType> inputTypes;
    private final long[] stateRetentionTime;
    private final List<Input<RowData>> typedInputs;
    // The multiJoinCondition is currently not being used, since we check the join conditions
    // for each while iterating through records to shortcircuit the recursion. However, if we
    // eventually want to cache join results at some level or do some other optimizations, this
    // might become useful.
    private final MultiJoinCondition multiJoinCondition;

    private final GeneratedJoinCondition[] joinConditions;
    private final JoinKeyExtractor keyExtractor;

    private transient List<MultiJoinStateView> stateHandlers;
    private transient TimestampedCollector<RowData> collector;
    private transient List<RowData> nullRows;
    private transient JoinCondition[] instantiatedJoinConditions;

    public StreamingMultiJoinOperator(
            StreamOperatorParameters<RowData> parameters,
            List<RowType> inputTypes,
            List<JoinInputSideSpec> inputSpecs,
            List<FlinkJoinType> joinTypes,
            MultiJoinCondition multiJoinCondition,
            long[] stateRetentionTime,
            GeneratedJoinCondition[] joinConditions,
            JoinKeyExtractor keyExtractor,
            // We currently don't use this, but it might be useful in the future for optimizations
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap) {
        super(parameters, inputSpecs.size());
        this.inputTypes = inputTypes;
        this.inputSpecs = inputSpecs;
        this.joinTypes = joinTypes;
        this.stateRetentionTime = stateRetentionTime;
        this.joinConditions = joinConditions;
        this.keyExtractor = keyExtractor;
        this.typedInputs = new ArrayList<>(inputSpecs.size());
        this.multiJoinCondition = multiJoinCondition;

        initializeInputs();
    }

    @Override
    public void open() throws Exception {
        super.open();
        initializeCollector();
        initializeNullRows();
        initializeStateHandlers();
        initializeJoinConditions();
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
        recursiveMultiJoin(0, input, inputId, null, false);
    }

    /**
     * See {@link StreamingMultiJoinOperator} for a detailed explanation of the recursive join and
     * examples.
     *
     * @param depth The current depth of the recursion, representing the input stream index (0 to
     *     N-1).
     * @param input The original input record that triggered this join operation.
     * @param inputId The index of the input stream from which the `input` record originated.
     * @param joinedRowData An array holding the candidate row from each input stream processed so
     *     far in this recursive path. `joinedRowData[d]` holds the row from input `d`.
     * @param isInputRecordActive True when `joinedRowData` contains the new added input record at
     *     its `inputId` (via `processInputRecord`), and we are in the mode to emit matching join
     *     row combinations. False if we are in the initial state exploration/association
     *     calculation mode.
     * @throws Exception If state access or condition evaluation fails.
     */
    private void recursiveMultiJoin(
            int depth,
            RowData input,
            int inputId,
            RowData joinedRowData,
            boolean isInputRecordActive)
            throws Exception {
        // Base case: If we've processed all inputs and reached the last level, all join conditions
        // for each level have matched. We now emit the joined output record.
        if (isMaxDepth(depth)) {
            emitJoinedRow(input, joinedRowData);
            return; // Return the count for the level below maxDepth
        }

        boolean isLeftJoin = isLeftJoinAtDepth(depth);

        // We store associations here because we need to know if we need to do emit
        // a null padded output if there were no matching records.
        // processRecords returns null if no state records for the next depth matched
        // joinedRowData. Otherwise, it returns the association count.
        Integer associations =
                processRecords(
                        depth, input, inputId, joinedRowData, isInputRecordActive, isLeftJoin);

        boolean anyMatches; // True if any state record at the current depth matched the
        // joinedRowData to
        // the left
        int associationsPrevLevel; // Association count for the joinedRowData to the left with
        // state at current depth

        if (associations == null) {
            // When associations are null, we did not emit any row: neither retractions nor inserts.
            // This means no state record at the current depth matched the joinedRowData to the
            // left.
            anyMatches = false;
            associationsPrevLevel = 0;
        } else {
            // At least one state combination matched the joinedRowData to the left
            // This means at least one state record at the current depth matched
            // the joinedRowData to the left.
            anyMatches = true;
            associationsPrevLevel = associations; // Get the calculated count
        }

        // If the current depth is the one where the triggering input record arrived,
        // now process the input record itself with the current combination of rows we are at.
        // processInputRecord will handle transitioning to the "emit results" mode
        // (isInputRecordActive =
        // true for its recursive calls).
        if (isInputLevel(depth, inputId)) {
            processInputRecord(
                    depth, input, inputId, joinedRowData, associationsPrevLevel, anyMatches);
        } else if (isLeftJoin && !anyMatches && hasNoAssociations(depth, associationsPrevLevel)) {
            // For LEFT joins, if no matches were found in the state for the joinedRowData to the
            // left
            // (anyMatches = false, associationsPrevLevel = 0 from state),
            // and overall associations for the joinedRowData to the left are zero,
            // process with null padding for the current depth.
            // In other words, we emit null for this level. This is important so we continue to the
            // join
            // with the output of this join level, which is a null padded row so we can eventually
            // reach the last join level.
            // Continue with the same isInputRecordActive mode.
            processWithNullPadding(depth, input, inputId, joinedRowData, isInputRecordActive);
        }
    }

    // This simply emits the resulting join row between all n inputs.
    private void emitJoinedRow(RowData input, RowData joinedRowData) {
        joinedRowData.setRowKind(input.getRowKind());
        collector.collect(joinedRowData);
    }

    /**
     * Processes records from the state for the current join depth.
     *
     * @param depth The current depth of recursion.
     * @param input The original input record.
     * @param inputId The ID of the input stream for the original input.
     * @param joinedRowData The current set of rows forming the join combination.
     * @param isInputRecordActive Whether the input record is currently active in `joinedRowData`.
     * @param isLeftJoin True if the join at the current depth is a LEFT join.
     * @return An {@code Integer} representing the association count for `joinedRowData[depth-1]`
     *     with matching records from `state[depth]`. Returns {@code null} if no records from
     *     `state[depth]` matched `joinedRowData[depth-1]`.
     * @throws Exception If state access or condition evaluation fails.
     */
    private Integer processRecords(
            int depth,
            RowData input,
            int inputId,
            RowData joinedRowData,
            boolean isInputRecordActive,
            boolean isLeftJoin)
            throws Exception {
        // We need both because associations can be 0 at the end even though we matched records.
        // For example, if we had 1 association and retracted it, associations are 0 but the
        // retraction
        // was actually a "match".
        // 'anyMatch' tracks if any record from state at current depth matched the joinedRowData to
        // the left.
        boolean anyMatch = false;
        // 'associations' counts matches between the joinedRowData to the left and state at current
        // depth for left
        // joins.
        int associations = 0;

        // If an inner join and we reached the input level, we don't have to count the number of
        // associations that the left side has. This is only necessary for left joins to know
        // if we have to retract or insert null padded rows for the incoming record.
        // In other rows, we do not have to process any records in state for the inputId if
        // it's an inner join, since we do not need to know if we the left side has associations.
        if (isInnerJoin(isLeftJoin) && isInputLevel(depth, inputId)) {
            // No associations relevant from state for this specific case, and no match from state
            // processing.
            return null;
        }

        // Calculate the joinKey to retrieve from the state only the records that can potentially
        // match based on the equi-join conditions.
        // The joinKey consists of all attributes present in equi-join conditions for the current
        // level.
        // We use the left side (joinedRowData) to calculate its value.
        RowData joinKey = keyExtractor.getLeftSideJoinKey(depth, joinedRowData);
        Iterable<RowData> records = stateHandlers.get(depth).getRecords(joinKey);

        for (RowData record : records) {
            // Shortcircuit: if the join condition fails, this path yields no results
            // we can go to the next record in the state.
            if (matchesCondition(depth, joinedRowData, record)) {
                anyMatch = true;
            } else {
                continue;
            }

            // For LEFT joins, association counts are updated for the preceding level (depth - 1)
            // to correctly track if the left-side row found any matches on this right-side.
            // This information is crucial for determining if null padding is needed later.
            if (isLeftJoin) {
                associations =
                        updateAssociationCount(
                                associations,
                                shouldIncrementAssociation(isInputRecordActive, input));

                // Optimization: further recursion or counting might be skippable under
                // specific conditions detailed in `canOptimizeAssociationCounting`.
                if (canOptimizeAssociationCounting(depth, inputId, input, associations)) {
                    // A match occurred, and we can optimize. Return the count.
                    return associations;
                }
            }

            // For the `inputId` level and when !isInputRecordActive (i.e., association calculation
            // mode),
            // the primary goal is to determine associations for the input record's level so we know
            // if we have to handle null padded retractions or insertions. Recursion to deeper
            // levels (joins to the right) is not needed for this specific count at this stage, as
            // the
            // input record's participation and further joins are handled by
            // `processInputRecord` or subsequent recursive calls where isInputRecordActive will be
            // true.
            if (!isInputRecordActive && isInputLevel(depth, inputId)) {
                continue;
            }

            RowData newJoinedRowData = newJoinedRowData(depth, joinedRowData, record);
            recursiveMultiJoin(depth + 1, input, inputId, newJoinedRowData, isInputRecordActive);
            // The returned associationCountForCurrentDepth is for recursiveMultiJoin
            // [depth], and its
            // updates
            // are handled within that recursive call. We don't use it to modify
            // currentAssociationCount (which is for joinedRowData[depth-1]).
        }

        // Returns whether any record at this level matched the local condition.
        if (!anyMatch) {
            return null; // No matches found in state for the preceding level.
        } else {
            return associations; // Matches found, return their count.
        }
    }

    private static RowData newJoinedRowData(int depth, RowData joinedRowData, RowData record) {
        RowData newJoinedRowData;
        if (depth == 0) {
            newJoinedRowData = record;
        } else {
            newJoinedRowData = new JoinedRowData(joinedRowData, record);
        }
        return newJoinedRowData;
    }

    private static boolean isInnerJoin(boolean isLeftJoin) {
        return !isLeftJoin;
    }

    private void processWithNullPadding(
            int depth,
            RowData input,
            int inputId,
            RowData joinedRowData,
            boolean isInputRecordActive)
            throws Exception {

        // Recursion continues with a null row at the current depth. This means the current join
        // emits a null padded output which is used for the next levels.
        // By continuing the recursion, we allow those deeper conditions to be evaluated and
        // ensures that the null padding correctly propagates to the join chain.

        RowData newJoinedRowData = newJoinedRowData(depth, joinedRowData, nullRows.get(depth));
        // When recursing for depth+1, the association count needed is for the current
        // joinedRowData.
        // Initialize it to 0.
        recursiveMultiJoin(
                depth + 1,
                input,
                inputId,
                newJoinedRowData,
                // Initial association count for the current joinedRowData
                isInputRecordActive);
    }

    /**
     * Processes the actual input record at its designated depth.
     *
     * @param depth The current depth, which should be equal to `inputId`.
     * @param input The original input record.
     * @param inputId The ID of the input stream.
     * @param joinedRowData The current set of rows forming the join combination.
     * @param associationsToPrevLevel The association count for `joinedRowData[depth-1]` as
     *     determined from processing state records at `state[depth]`.
     * @param anyMatch True if any record from `state[depth]` matched `joinedRowData[depth-1]`.
     * @throws Exception If state access or condition evaluation fails.
     */
    private void processInputRecord(
            int depth,
            RowData input,
            int inputId,
            RowData joinedRowData,
            int associationsToPrevLevel,
            boolean anyMatch)
            throws Exception {
        // 'associations' starts with the count from state and is updated by this input record.
        // It represents the total associations for joinedRowData[depth-1] with (state[depth] + this
        // input).
        int associations = associationsToPrevLevel;
        // if the join condition fails, this path yields no results.
        if (!matchesCondition(depth, joinedRowData, input)) {
            return; // No match, nothing more to do on this path
        }

        boolean isLeftJoin = isLeftJoinAtDepth(depth);
        // When processing the actual input record, if it satisfies the outer join condition
        // for this level (`depth`), its association with the preceding level (`depth-1`)
        // must be updated: either increment if upsert or decrement the number of associations, if a
        // retraction. This happens in the "result emission mode" (isInputRecordActive = true
        // implicitly for this logic path).
        if (isLeftJoin) {
            associations =
                    updateAssociationCount(
                            associations,
                            shouldIncrementAssociation(
                                    true, input)); // true for isInputRecordActive
        }

        // --- Left Join Retraction Handling ---
        // For an incoming INSERT/UPDATE_AFTER on the right side of a LEFT join,
        // if the corresponding left-side row previously had no matches (indicated by `!anyMatch`
        // which means no state records at the current joinedRowData matched joinedRowData[depth-1])
        // during
        // the association calculation phase where isInputRecordActive was false),
        // it would have resulted in a null-padded output. This new record might form a
        // valid join, so the previous null-padded output must be retracted.
        if (isUpsert(input) && isLeftJoin && !anyMatch) {
            handleRetractBeforeInput(depth, input, inputId, joinedRowData);
        }

        // Continue recursion to the next depth. Crucially, `isInputRecordActive` is now true
        // because we have incorporated the actual input record, and any further matches
        // found should lead to output generation or retractions.
        RowData newJoinedRowData = newJoinedRowData(depth, joinedRowData, input);
        // When recursing for depth+1, the association count needed is for the current
        // joinedRowData.
        // Initialize it to 0.
        recursiveMultiJoin(
                depth + 1, input, inputId, newJoinedRowData, true); // true for isInputRecordActive

        // --- Left Join Insertion Handling ---
        // If an incoming DELETE/UPDATE_BEFORE on the right side of a LEFT join removes
        // the last matching record for the left-side row (checked via `hasNoAssociations`
        // using the total 'associations' count for joinedRowData[depth-1]),
        // then according to LEFT join semantics, a new null-padded result must be inserted.
        if (isRetraction(input) && isLeftJoin && hasNoAssociations(depth, associations)) {
            handleInsertAfterInput(depth, input, inputId, joinedRowData);
        }
    }

    private void handleRetractBeforeInput(
            int depth, RowData input, int inputId, RowData joinedRowData) throws Exception {
        // To construct the row that needs retraction, temporarily place a null row here.
        JoinedRowData newJoinedRowData = new JoinedRowData(joinedRowData, nullRows.get(depth));
        RowKind originalKind = input.getRowKind();
        // Temporarily change RowKind to DELETE to trigger retraction downstream.
        input.setRowKind(RowKind.DELETE);

        // Recurse to emit the potential retraction for the previously null-padded row.
        // This is part of processing the input record, so isInputRecordActive = true.
        // When recursing for depth+1, the association count needed is for the current
        // joinedRowData.
        // Initialize it to 0.
        recursiveMultiJoin(
                depth + 1,
                input,
                inputId,
                newJoinedRowData,
                // Initial association count for the current joinedRowData
                true); // true for isInputRecordActive

        // Restore the input record's original RowKind to prevent unintended side effects,
        // as the `input` object itself was temporarily modified.
        input.setRowKind(originalKind);
    }

    private void handleInsertAfterInput(
            int depth, RowData input, int inputId, RowData joinedRowData) throws Exception {
        // To construct the new null-padded row, temporarily place a null row here.
        JoinedRowData newJoinedRowData = new JoinedRowData(joinedRowData, nullRows.get(depth));
        RowKind originalKind = input.getRowKind();
        // Temporarily change RowKind to INSERT to trigger insertion downstream.
        input.setRowKind(RowKind.INSERT);

        // Recurse to emit the potential insertion for the new null-padded row.
        // This is part of processing the input record, so isInputRecordActive = true.
        // When recursing for depth+1, the association count needed is for the current
        // joinedRowData.
        // Initialize it to 0.
        recursiveMultiJoin(
                depth + 1,
                input,
                inputId,
                newJoinedRowData,
                // Initial association count for the current joinedRowData
                true); // true for isInputRecordActive

        // Restore the input record's original RowKind to prevent unintended side effects,
        // as the `input` object itself was temporarily modified.
        input.setRowKind(originalKind);
    }

    private void addRecordToState(RowData input, int inputId) throws Exception {
        final boolean isUpsert = isUpsert(input);
        RowData joinKey = keyExtractor.getJoinKey(input, inputId);

        // Always use insert so we store and retract records correctly from state
        input.setRowKind(RowKind.INSERT);
        if (isUpsert) {
            stateHandlers.get(inputId).addRecord(joinKey, input);
        } else {
            stateHandlers.get(inputId).retractRecord(joinKey, input);
        }
    }

    private void initializeCollector() {
        this.collector = new TimestampedCollector<>(output);
    }

    private void initializeNullRows() {
        this.nullRows = new ArrayList<>(inputTypes.size());
        for (RowType inputType : inputTypes) {
            this.nullRows.add(new GenericRowData(inputType.getFieldCount()));
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
            RowType joinKeyType = keyExtractor.getJoinKeyType(i);

            stateView =
                    MultiJoinStateViews.create(
                            getRuntimeContext(),
                            stateName,
                            inputSpecs.get(i),
                            joinKeyType,
                            inputTypes.get(i),
                            stateRetentionTime[i]);
            stateHandlers.add(stateView);
        }
    }

    private void initializeInputs() {
        for (int i = 0; i < inputSpecs.size(); i++) {
            typedInputs.add(createInput(i + 1));
        }
    }

    private void closeConditions() throws Exception {
        if (multiJoinCondition != null) {
            multiJoinCondition.close();
        }
        if (instantiatedJoinConditions != null) {
            for (final JoinCondition jc : instantiatedJoinConditions) {
                if (jc != null) {
                    jc.close();
                }
            }
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

    private boolean isUpsert(RowData row) {
        return row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER;
    }

    private boolean isRetraction(RowData row) {
        return row.getRowKind() == RowKind.DELETE || row.getRowKind() == RowKind.UPDATE_BEFORE;
    }

    private boolean isLeftJoinAtDepth(int depth) {
        return depth > 0 && joinTypes.get(depth) == FlinkJoinType.LEFT;
    }

    /** Checks if the join condition specific to the current depth holds true. */
    private boolean matchesCondition(int depth, RowData joinedRowData, RowData record) {
        // The first input (depth 0) doesn't have a preceding input to join with,
        // so there's no specific join condition to evaluate against `joinConditions[0]`.
        return depth == 0 || instantiatedJoinConditions[depth].apply(joinedRowData, record);
    }

    private int updateAssociationCount(int currentCount, boolean isUpsert) {
        // This method is called when depth > 0 (implicitly by isLeftJoin checks by callers)
        if (isUpsert) {
            return currentCount + 1;
        } else {
            return currentCount - 1;
        }
    }

    private boolean shouldIncrementAssociation(boolean isInputRecordActive, RowData input) {
        // If not processing the active input record (i.e., in association calculation mode),
        // we always effectively "increment" for the purpose of counting potential matches from
        // state.
        // If processing the active input record, increment only for upserts.
        return !isInputRecordActive || isUpsert(input);
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

    private boolean hasNoAssociations(int depth, int associationCountForPrevLevel) {
        return depth > 0 && associationCountForPrevLevel == 0;
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
            int depth, int inputId, RowData input, int associationCountForPrevLevel) {
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
            return associationCountForPrevLevel > 0;
        } else {
            // For a retraction, if more than one match existed (associations > 1),
            // removing this one input record means other matches still exist.
            // Therefore, we won't need to insert a new null-padded row after this retraction.
            // If associations[depth-1] was 1, then after decrementing it becomes 0,
            // and we would need to insert a null padded row.
            return associationCountForPrevLevel > 1;
        }
    }

    private void initializeJoinConditions() throws Exception {
        this.instantiatedJoinConditions = new JoinCondition[joinConditions.length];
        for (int i = 0; i < joinConditions.length; i++) {
            if (this.joinConditions[i] != null) {
                final JoinCondition cond =
                        this.joinConditions[i].newInstance(
                                getRuntimeContext().getUserCodeClassLoader());
                cond.setRuntimeContext(getRuntimeContext());
                cond.open(DefaultOpenContext.INSTANCE);
                this.instantiatedJoinConditions[i] = cond;
            }
        }
    }
}
