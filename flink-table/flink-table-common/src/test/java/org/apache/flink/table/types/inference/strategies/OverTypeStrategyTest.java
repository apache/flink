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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.OverWindowRange;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.math.BigDecimal;
import java.util.stream.Stream;

/** Tests for {@link OverTypeStrategy}. */
class OverTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final DataType TIME_ATTRIBUTE_TYPE =
            TypeConversions.fromLogicalToDataType(
                    new TimestampType(false, TimestampKind.ROWTIME, 3));

    private static final DataType SYMBOL_TYPE =
            TypeConversions.fromLogicalToDataType(new SymbolType<>());

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy("Row window literals", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(), TIME_ATTRIBUTE_TYPE, SYMBOL_TYPE, SYMBOL_TYPE)
                        .calledWithLiteralAt(2, OverWindowRange.UNBOUNDED_ROW)
                        .calledWithLiteralAt(3, OverWindowRange.UNBOUNDED_ROW)
                        .expectArgumentTypes(
                                DataTypes.INT(), TIME_ATTRIBUTE_TYPE, SYMBOL_TYPE, SYMBOL_TYPE),
                TestSpec.forStrategy("Range window literals", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(), TIME_ATTRIBUTE_TYPE, SYMBOL_TYPE, SYMBOL_TYPE)
                        .calledWithLiteralAt(2, OverWindowRange.CURRENT_RANGE)
                        .calledWithLiteralAt(3, OverWindowRange.UNBOUNDED_RANGE)
                        .expectArgumentTypes(
                                DataTypes.INT(), TIME_ATTRIBUTE_TYPE, SYMBOL_TYPE, SYMBOL_TYPE),
                TestSpec.forStrategy(
                                "Row window literal with row number following",
                                SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.BIGINT())
                        .calledWithLiteralAt(2, OverWindowRange.CURRENT_ROW)
                        .calledWithLiteralAt(3, 10L)
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.BIGINT()),
                TestSpec.forStrategy(
                                "Row window literal with row number preceding",
                                SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.BIGINT(),
                                SYMBOL_TYPE)
                        .calledWithLiteralAt(2, 10L)
                        .calledWithLiteralAt(3, OverWindowRange.CURRENT_ROW)
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.BIGINT(),
                                SYMBOL_TYPE),
                TestSpec.forStrategy(
                                "Row window literal with row number preceding and following",
                                SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT())
                        .calledWithLiteralAt(2, 10L)
                        .calledWithLiteralAt(3, 10L)
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT()),
                TestSpec.forStrategy(
                                "Range window literal with time following",
                                SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()))
                        .calledWithLiteralAt(2, OverWindowRange.CURRENT_RANGE)
                        .calledWithLiteralAt(3, BigDecimal.valueOf(10L))
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND())),
                TestSpec.forStrategy(
                                "Range window literal with time preceding",
                                SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()),
                                SYMBOL_TYPE)
                        .calledWithLiteralAt(2, BigDecimal.valueOf(10L))
                        .calledWithLiteralAt(3, OverWindowRange.CURRENT_RANGE)
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()),
                                SYMBOL_TYPE),
                TestSpec.forStrategy(
                                "Range window literal with time preceding and following",
                                SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()),
                                DataTypes.INTERVAL(DataTypes.SECOND()))
                        .calledWithLiteralAt(2, BigDecimal.valueOf(10L))
                        .calledWithLiteralAt(3, BigDecimal.valueOf(10L))
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()),
                                DataTypes.INTERVAL(DataTypes.SECOND())),
                TestSpec.forStrategy(
                                "Different window kind literals", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(), TIME_ATTRIBUTE_TYPE, SYMBOL_TYPE, SYMBOL_TYPE)
                        .calledWithLiteralAt(2, OverWindowRange.CURRENT_ROW)
                        .calledWithLiteralAt(3, OverWindowRange.UNBOUNDED_RANGE)
                        .expectErrorMessage(
                                "Preceding and following must be of same interval type."),
                TestSpec.forStrategy("Non literal preceding", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.BIGINT(),
                                SYMBOL_TYPE)
                        .calledWithLiteralAt(3, OverWindowRange.UNBOUNDED_ROW)
                        .expectErrorMessage(
                                "Preceding must be a row interval or time interval literal."),
                TestSpec.forStrategy("Non literal following", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.BIGINT())
                        .calledWithLiteralAt(2, OverWindowRange.UNBOUNDED_ROW)
                        .expectErrorMessage(
                                "Following must be a row interval or time interval literal."),
                TestSpec.forStrategy("Negative preceding", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.BIGINT(),
                                SYMBOL_TYPE)
                        .calledWithLiteralAt(2, -2L)
                        .calledWithLiteralAt(3, OverWindowRange.UNBOUNDED_ROW)
                        .expectErrorMessage("Preceding row interval must be larger than 0."),
                TestSpec.forStrategy("Negative following", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.BIGINT())
                        .calledWithLiteralAt(2, OverWindowRange.UNBOUNDED_ROW)
                        .calledWithLiteralAt(3, -2L)
                        .expectErrorMessage("Following row interval must be larger than 0."),
                TestSpec.forStrategy("Negative time preceding", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()),
                                SYMBOL_TYPE)
                        .calledWithLiteralAt(2, BigDecimal.valueOf(-10L))
                        .calledWithLiteralAt(3, OverWindowRange.UNBOUNDED_RANGE)
                        .expectErrorMessage(
                                "Preceding time interval must be equal or larger than 0."),
                TestSpec.forStrategy("Zero time following", SpecificInputTypeStrategies.OVER)
                        .calledWithArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND()))
                        .calledWithLiteralAt(2, OverWindowRange.UNBOUNDED_RANGE)
                        .calledWithLiteralAt(3, BigDecimal.valueOf(0))
                        .expectArgumentTypes(
                                DataTypes.INT(),
                                TIME_ATTRIBUTE_TYPE,
                                SYMBOL_TYPE,
                                DataTypes.INTERVAL(DataTypes.SECOND())));
    }
}
