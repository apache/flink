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
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.logical.SymbolType;

import org.junit.runners.Parameterized;

import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.symbol;

/** Tests for {@link SymbolArgumentTypeStrategy}. */
public class SymbolArgumentTypeStrategyTest extends InputTypeStrategiesTestBase {

    private static final InputTypeStrategy STRATEGY = sequence(symbol(TestEnum.class));

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return asList(
                TestSpec.forStrategy("Valid argument", STRATEGY)
                        .calledWithArgumentTypes(makeEnumType(TestEnum.class))
                        .calledWithLiteralAt(0, TestEnum.A)
                        .expectSignature("f(<TestEnum>)"),
                TestSpec.forStrategy("Wrong enum", STRATEGY)
                        .calledWithArgumentTypes(makeEnumType(InvalidEnum.class))
                        .calledWithLiteralAt(0, InvalidEnum.A)
                        .expectErrorMessage(
                                "Unsupported argument symbol type. "
                                        + "Expected symbol 'TestEnum' but actual symbol was 'InvalidEnum'."),
                TestSpec.forStrategy("Wrong type", STRATEGY)
                        .calledWithArgumentTypes(DataTypes.STRING())
                        .expectErrorMessage(
                                "Unsupported argument type. "
                                        + "Expected symbol type 'TestEnum' but actual type was 'STRING'."));
    }

    private static <T extends TableSymbol> DataType makeEnumType(Class<T> enumClass) {
        return new AtomicDataType(new SymbolType<T>(enumClass));
    }

    private enum TestEnum implements TableSymbol {
        A,
        B,
    }

    private enum InvalidEnum implements TableSymbol {
        A
    }
}
