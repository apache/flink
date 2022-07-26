/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.hint;

import org.apache.flink.table.planner.plan.rules.logical.WrapJsonAggFunctionArgumentsRule;

import org.apache.calcite.rel.hint.HintOptionChecker;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.util.Litmus;

import java.util.Collections;

/** A collection of Flink style {@link HintStrategy}s. */
public abstract class FlinkHintStrategies {

    /**
     * Customize the {@link HintStrategyTable} which contains hint strategies supported by Flink.
     */
    public static HintStrategyTable createHintStrategyTable() {
        return HintStrategyTable.builder()
                // Configure to always throw when we encounter any hint errors
                // (either the non-registered hint or the hint format).
                .errorHandler(Litmus.THROW)
                .hintStrategy(
                        FlinkHints.HINT_NAME_OPTIONS,
                        HintStrategy.builder(HintPredicates.TABLE_SCAN)
                                .optionChecker(
                                        (hint, errorHandler) ->
                                                errorHandler.check(
                                                        hint.kvOptions.size() > 0,
                                                        "Hint [{}] only support non empty key value options",
                                                        hint.hintName))
                                .build())
                .hintStrategy(
                        FlinkHints.HINT_NAME_JSON_AGGREGATE_WRAPPED,
                        HintStrategy.builder(HintPredicates.AGGREGATE)
                                .excludedRules(WrapJsonAggFunctionArgumentsRule.INSTANCE)
                                .build())
                // internal join hint used for alias
                .hintStrategy(
                        FlinkHints.HINT_ALIAS,
                        HintStrategy.builder(TAG_PREDICATE)
                                .optionChecker(fixedSizeListOptionChecker(1))
                                .build())
                // TODO semi/anti join with CORRELATE is not supported
                .hintStrategy(
                        JoinStrategy.BROADCAST.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.SHUFFLE_HASH.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.SHUFFLE_MERGE.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .hintStrategy(
                        JoinStrategy.NEST_LOOP.getJoinHintName(),
                        HintStrategy.builder(HintPredicates.JOIN)
                                .optionChecker(NON_EMPTY_LIST_OPTION_CHECKER)
                                .build())
                .build();
    }

    private static HintOptionChecker fixedSizeListOptionChecker(int size) {
        return (hint, errorHandler) ->
                errorHandler.check(
                        hint.listOptions.size() == size,
                        "Invalid hint: {}, expecting {} table or view {} "
                                + "specified in hint {}.",
                        FlinkHints.stringifyHints(Collections.singletonList(hint)),
                        size,
                        size > 1 ? "names" : "name",
                        hint.hintName);
    }

    // ~ hint option checker ------------------------------------------------------------

    private static final HintOptionChecker NON_EMPTY_LIST_OPTION_CHECKER =
            (hint, errorHandler) ->
                    errorHandler.check(
                            hint.listOptions.size() > 0,
                            "Invalid hint: {}, expecting at least "
                                    + "one table or view specified in hint {}.",
                            FlinkHints.stringifyHints(Collections.singletonList(hint)),
                            hint.hintName);

    // ~ hint predicate ------------------------------------------------------------

    // For hint used as tag, and tag predicate never propagate.
    private static final HintPredicate TAG_PREDICATE = (hint, node) -> false;
}
