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
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.table.runtime.operators.join.stream.state.OuterMultiJoinStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterMultiJoinStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
    private final List<Integer> levels;
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
            Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            List<Integer> levels) {
        super(parameters, inputSpecs.size());
        this.inputTypes = inputTypes;
        this.inputSpecs = inputSpecs;
        this.joinTypes = joinTypes;
        this.stateRetentionTime = stateRetentionTime;
        this.joinConditions = joinConditions;
        this.keyExtractor = keyExtractor;
        this.typedInputs = new ArrayList<>(inputSpecs.size());
        this.multiJoinCondition = multiJoinCondition;
        this.levels = levels;

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

        int associationCount = performMultiJoin(input, inputId);
        addRecordToState(input, inputId, associationCount);
    }

    private int performMultiJoin(RowData input, int inputId) throws Exception {
        Iterable<RowData> outputRecords =
                recursiveMultiJoin(0, inputSpecs.size() - 1, inputId, input);

        for (RowData record : outputRecords) {
            collector.collect(record);
        }

        return ((CountingIterable) outputRecords).getCount();
    }

    private Iterable<RowData> recursiveMultiJoin(
            int leftIdx, int rightIdx, int inputId, RowData input) throws Exception {
        if (levels.get(leftIdx) < levels.get(rightIdx)) {
            return handleJoinedRowDataOnTheLeft(
                    joinTypes.get(rightIdx), leftIdx, rightIdx, inputId, input);
        } else if (levels.get(leftIdx) > levels.get(rightIdx)) {
            return handleJoinedRowDataOnTheRight(
                    joinTypes.get(leftIdx), leftIdx, rightIdx, inputId, input);
        } else {
            return handleLowestLevel(joinTypes.get(rightIdx), leftIdx, rightIdx, inputId, input);
        }
    }

    private Iterable<RowData> handleJoinedRowDataOnTheRight(
            FlinkJoinType joinType, int leftIdx, int rightIdx, int inputId, RowData input)
            throws Exception {
        Iterable<Tuple2<RowData, Integer>> leftRecords;

        if (leftIdx == inputId) {
            leftRecords = Collections.singletonList(Tuple2.of(input, -1));
            return outerJoinedIterableFromLeft(
                    joinType, leftRecords, input, leftIdx, rightIdx, inputId, true);
        } else if (inputId > leftIdx && inputId <= rightIdx) {
            Iterable<RowData> rightRecords =
                    recursiveMultiJoin(leftIdx + 1, rightIdx, inputId, input);
            return joinedIterableFromRight(rightRecords, leftIdx, rightIdx);
        } else {
            leftRecords =
                    ((OuterMultiJoinStateView) stateHandlers.get(leftIdx))
                            .getRecordsAndNumOfAssociations(null);
            return outerJoinedIterableFromLeft(
                    joinType, leftRecords, input, leftIdx, rightIdx, inputId, false);
        }
    }

    private Iterable<RowData> handleJoinedRowDataOnTheLeft(
            FlinkJoinType joinType, int leftIdx, int rightIdx, int inputId, RowData input)
            throws Exception {
        Iterable<RowData> leftRecords = recursiveMultiJoin(leftIdx, rightIdx - 1, inputId, input);

        if (rightIdx == inputId) {
            return joinedIterableFromLeft(joinType, leftRecords, input, rightIdx, true);
        } else {
            return joinedIterableFromLeft(joinType, leftRecords, input, rightIdx, false);
        }
    }

    private Iterable<RowData> handleLowestLevel(
            FlinkJoinType joinType, int leftIdx, int rightIdx, int inputId, RowData input)
            throws Exception {
        Iterable<RowData> leftRecords;
        if (rightIdx == inputId) {
            leftRecords = stateHandlers.get(leftIdx).getRecords(null);
            return joinedIterableFromLeft(joinType, leftRecords, input, rightIdx, true);
        } else if (leftIdx == inputId) {
            leftRecords = Collections.singletonList(input);
            return joinedIterableFromLeft(joinType, leftRecords, input, rightIdx, false);
        } else {
            leftRecords = stateHandlers.get(leftIdx).getRecords(null);
            return joinedIterableFromLeft(joinType, leftRecords, input, rightIdx, false);
        }
    }

    private Iterable<RowData> joinedIterableFromRight(
            Iterable<RowData> rightRecords, int leftIdx, int rightIdx) {
        return new CountingIterable<RowData>() {
            @Override
            public int getCount() {
                if (rightRecords instanceof CountingIterable) {
                    return ((CountingIterable<RowData>) rightRecords).getCount();
                }
                return -1;
            }

            @Override
            public Iterator<RowData> iterator() {
                return new Iterator<RowData>() {
                    Iterator<RowData> rightIterator = rightRecords.iterator();
                    Iterator<Tuple2<RowData, Tuple2<Integer, Integer>>> leftIterator =
                            Collections.emptyIterator();

                    boolean needInsert = false;

                    RowData currentRight = null;
                    Tuple2<RowData, Tuple2<Integer, Integer>> currentLeft = null;
                    RowData joinKey = null;

                    @Override
                    public boolean hasNext() {
                        if (currentLeft != null) {
                            return true;
                        }

                        while ((leftIterator == null || !leftIterator.hasNext())
                                && rightIterator.hasNext()) {
                            currentRight = rightIterator.next();
                            Iterable<Tuple2<RowData, Tuple2<Integer, Integer>>> leftIterable;
                            joinKey = keyExtractor.getJoinedSideJoinKey(leftIdx, currentRight);
                            try {
                                leftIterable =
                                        ((OuterMultiJoinStateView) stateHandlers.get(leftIdx))
                                                .getRecordsCountAndNumOfAssociations(joinKey);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            leftIterator =
                                    createMatchingIteratorOnTheRight(
                                            leftIterable.iterator(), currentRight, leftIdx);
                        }

                        return leftIterator != null && leftIterator.hasNext();
                    }

                    @Override
                    public RowData next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        if (currentLeft != null) {
                            if (!needInsert) {
                                Tuple2<RowData, Tuple2<Integer, Integer>> tmp = currentLeft;
                                currentLeft = null;

                                return new JoinedRowData(RowKind.INSERT, tmp.f0, currentRight);
                            } else {
                                needInsert = false;
                                RowData nullRow = getNullRowFromLeftToRight(leftIdx + 1, rightIdx);
                                Tuple2<RowData, Tuple2<Integer, Integer>> tmp = currentLeft;
                                currentLeft = null;

                                return new JoinedRowData(RowKind.INSERT, tmp.f0, nullRow);
                            }
                        }

                        RowKind rowKind = currentRight.getRowKind();
                        currentLeft = leftIterator.next();

                        if (isUpsert(currentRight) && currentLeft.f1.f1 == 0) {
                            RowData nullRow = getNullRowFromLeftToRight(leftIdx + 1, rightIdx);
                            updateAssociationCount(
                                    leftIdx,
                                    joinKey,
                                    currentLeft.f0,
                                    currentLeft.f1.f0,
                                    currentLeft.f1.f1 + 1);

                            return new JoinedRowData(RowKind.DELETE, currentLeft.f0, nullRow);
                        }

                        if (isRetraction(currentRight)) {
                            if (currentLeft.f1.f1 == 1) {
                                needInsert = true;
                            }

                            updateAssociationCount(
                                    leftIdx,
                                    joinKey,
                                    currentLeft.f0,
                                    currentLeft.f1.f0,
                                    currentLeft.f1.f1 - 1);

                            return new JoinedRowData(rowKind, currentLeft.f0, currentRight);
                        }

                        Tuple2<RowData, Tuple2<Integer, Integer>> tmp = currentLeft;
                        currentLeft = null;

                        return new JoinedRowData(currentRight.getRowKind(), tmp.f0, currentRight);
                    }
                };
            }
        };
    }

    private void updateAssociationCount(
            int idx, RowData joinKey, RowData record, int count, int associationCount) {
        try {
            ((OuterMultiJoinStateView) stateHandlers.get(idx))
                    .addRecord(joinKey, record, new Tuple2<>(count, associationCount));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Iterable<RowData> outerJoinedIterableFromLeft(
            FlinkJoinType joinType,
            Iterable<Tuple2<RowData, Integer>> leftRecords,
            RowData input,
            int leftIdx,
            int rightIdx,
            int inputId,
            boolean isInputOnTheLeft) {
        return new CountingIterable<>() {
            public int associationCount = -1;

            @Override
            public int getCount() {
                return associationCount;
            }

            @Override
            public Iterator<RowData> iterator() {
                return new Iterator<>() {
                    final Iterator<Tuple2<RowData, Integer>> leftIterator = leftRecords.iterator();
                    Iterator<RowData> rightIterator = Collections.emptyIterator();
                    Tuple2<RowData, Integer> currentLeft = null;

                    @Override
                    public boolean hasNext() {
                        while ((rightIterator == null || !rightIterator.hasNext())
                                && leftIterator.hasNext()) {
                            currentLeft = leftIterator.next();

                            if (joinType == FlinkJoinType.LEFT && currentLeft.f1 == 0) {
                                RowData nullRow = getNullRowFromLeftToRight(leftIdx + 1, rightIdx);
                                rightIterator = Collections.singletonList(nullRow).iterator();
                                break;
                            }

                            try {
                                rightIterator =
                                        createMatchingIterator(
                                                recursiveMultiJoin(
                                                                leftIdx + 1,
                                                                rightIdx,
                                                                inputId,
                                                                input)
                                                        .iterator(),
                                                currentLeft.f0,
                                                leftIdx);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }

                            if (joinType == FlinkJoinType.LEFT && isInputOnTheLeft) {
                                if (!rightIterator.hasNext()) {
                                    RowData nullRow =
                                            getNullRowFromLeftToRight(leftIdx + 1, rightIdx);
                                    rightIterator = Collections.singletonList(nullRow).iterator();
                                } else {
                                    associationCount = 0;
                                }
                            }
                        }
                        return rightIterator != null && rightIterator.hasNext();
                    }

                    @Override
                    public RowData next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        if (isInputOnTheLeft) {
                            associationCount++;
                        }
                        RowData currentRight = rightIterator.next();
                        RowKind recordKind = currentLeft.f0.getRowKind();
                        return new JoinedRowData(recordKind, currentLeft.f0, currentRight);
                    }
                };
            }
        };
    }

    private Iterable<RowData> joinedIterableFromLeft(
            FlinkJoinType joinType,
            Iterable<RowData> leftRecords,
            RowData input,
            int rightIdx,
            boolean isInputOnTheRight) {
        return new CountingIterable<>() {
            @Override
            public int getCount() {
                if (leftRecords instanceof CountingIterable) {
                    return ((CountingIterable<?>) leftRecords).getCount();
                }

                return -1;
            }

            @Override
            public Iterator<RowData> iterator() {
                return new Iterator<>() {
                    final Iterator<RowData> leftIterator = leftRecords.iterator();
                    Iterator<RowData> rightIterator = Collections.emptyIterator();
                    RowData currentLeft = null;

                    @Override
                    public boolean hasNext() {
                        while ((rightIterator == null || !rightIterator.hasNext())
                                && leftIterator.hasNext()) {
                            currentLeft = leftIterator.next();
                            RowData joinKey =
                                    keyExtractor.getJoinedSideJoinKey(rightIdx, currentLeft);

                            if (isInputOnTheRight) {
                                rightIterator =
                                        createMatchingIterator(
                                                Collections.singletonList(input).iterator(),
                                                currentLeft,
                                                rightIdx);
                            } else {
                                try {
                                    rightIterator =
                                            createMatchingIterator(
                                                    stateHandlers
                                                            .get(rightIdx)
                                                            .getRecords(joinKey)
                                                            .iterator(),
                                                    currentLeft,
                                                    rightIdx);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            if (joinType == FlinkJoinType.LEFT) {
                                if (!isInputOnTheRight) {
                                    rightIterator =
                                            handleInputOnTheLeft(
                                                    rightIterator, currentLeft, rightIdx, joinKey);
                                } else {
                                    rightIterator =
                                            handleInputOnTheRight(
                                                    rightIterator, currentLeft, rightIdx, joinKey);
                                }
                            }
                        }
                        return rightIterator != null && rightIterator.hasNext();
                    }

                    @Override
                    public RowData next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        RowData currentRight = rightIterator.next();
                        RowKind recordKind =
                                isInputOnTheRight
                                        ? currentRight.getRowKind()
                                        : currentLeft.getRowKind();
                        return new JoinedRowData(recordKind, currentLeft, currentRight);
                    }
                };
            }
        };
    }

    private Iterator<RowData> handleInputOnTheRight(
            Iterator<RowData> rightIterator, RowData currentLeft, int rightIdx, RowData joinKey) {
        if (!rightIterator.hasNext()) {
            return rightIterator;
        }
        RowData input = rightIterator.next();
        RowData nullRow = new GenericRowData(inputTypes.get(rightIdx).getFieldCount());

        Iterator<RowData> matchingIterator;
        try {
            matchingIterator =
                    createMatchingIterator(
                            stateHandlers.get(rightIdx).getRecords(joinKey).iterator(),
                            currentLeft,
                            rightIdx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (isUpsert(input) && !matchingIterator.hasNext()) {
            nullRow.setRowKind(RowKind.DELETE);
            return List.of(nullRow, input).iterator();
        }

        if (isRetraction(input)) {
            matchingIterator.next();
            if (!matchingIterator.hasNext()) {
                nullRow.setRowKind(RowKind.INSERT);
                return List.of(input, nullRow).iterator();
            }
        }

        return Collections.singletonList(input).iterator();
    }

    private Iterator<RowData> handleInputOnTheLeft(
            Iterator<RowData> rightIterator, RowData currentLeft, int rightIdx, RowData joinKey) {
        if (!rightIterator.hasNext()) {
            RowData nullRow = new GenericRowData(inputTypes.get(rightIdx).getFieldCount());
            return Collections.singletonList(nullRow).iterator();
        }

        return rightIterator;
    }

    private Iterator<RowData> createMatchingIterator(
            Iterator<RowData> rightIterator, RowData leftRecord, int idx) {
        return new Iterator<>() {
            RowData currentRight = null;

            @Override
            public boolean hasNext() {
                while (currentRight == null && rightIterator.hasNext()) {
                    RowData rightRecord = rightIterator.next();
                    if (instantiatedJoinConditions[idx].apply(leftRecord, rightRecord)) {
                        currentRight = rightRecord;
                    }
                }

                return currentRight != null;
            }

            @Override
            public RowData next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                RowData tmp = currentRight;
                currentRight = null;
                return tmp;
            }
        };
    }

    private Iterator<Tuple2<RowData, Tuple2<Integer, Integer>>> createMatchingIteratorOnTheRight(
            Iterator<Tuple2<RowData, Tuple2<Integer, Integer>>> leftIterator,
            RowData rightRecord,
            int idx) {
        return new Iterator<>() {
            Tuple2<RowData, Tuple2<Integer, Integer>> currentLeft = null;

            @Override
            public boolean hasNext() {
                while (currentLeft == null && leftIterator.hasNext()) {
                    Tuple2<RowData, Tuple2<Integer, Integer>> leftRecord = leftIterator.next();
                    if (instantiatedJoinConditions[idx].apply(leftRecord.f0, rightRecord)) {
                        currentLeft = leftRecord;
                    }
                }

                return currentLeft != null;
            }

            @Override
            public Tuple2<RowData, Tuple2<Integer, Integer>> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Tuple2<RowData, Tuple2<Integer, Integer>> tmp = currentLeft;
                currentLeft = null;
                return tmp;
            }
        };
    }

    private void addRecordToState(RowData input, int inputId, int associationCount)
            throws Exception {
        RowData joinKey = keyExtractor.getJoinKey(input, inputId);

        // Always use insert so we store and retract records correctly from state
        input.setRowKind(RowKind.INSERT);
        if (isRetraction(input)) {
            stateHandlers.get(inputId).retractRecord(joinKey, input);
        } else {
            if (associationCount >= 0) {
                OuterMultiJoinStateView stateView =
                        (OuterMultiJoinStateView) stateHandlers.get(inputId);
                stateView.addRecord(joinKey, input, associationCount);
            } else {
                stateHandlers.get(inputId).addRecord(joinKey, input);
            }
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

            if (i + 1 < inputSpecs.size() && levels.get(i) > levels.get(i + 1)) {
                stateView =
                        OuterMultiJoinStateViews.create(
                                getRuntimeContext(),
                                stateName,
                                inputSpecs.get(i),
                                joinKeyType,
                                inputTypes.get(i),
                                stateRetentionTime[i]);
            } else {
                stateView =
                        MultiJoinStateViews.create(
                                getRuntimeContext(),
                                stateName,
                                inputSpecs.get(i),
                                joinKeyType,
                                inputTypes.get(i),
                                stateRetentionTime[i]);
            }

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

    private RowData getNullRowFromLeftToRight(int leftIdx, int rightIdx) {
        int nullFieldsCount = 0;
        for (int i = leftIdx; i <= rightIdx; i++) {
            nullFieldsCount += inputTypes.get(i).getFieldCount();
        }

        return new GenericRowData(nullFieldsCount);
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

    private interface CountingIterable<T> extends Iterable<T> {
        default int getCount() {
            return -1;
        }
    }
}
