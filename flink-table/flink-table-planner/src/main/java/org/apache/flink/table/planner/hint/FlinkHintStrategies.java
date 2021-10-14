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

import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.util.Litmus;

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
                .build();
    }
}
