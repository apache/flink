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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.VariantType;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.ARRAY_EQUALS_COMPARABLE;
import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.MAP_KEYS_EQUALS_COMPARABLE;

/** Tests for {@link EqualsComparableElementArgumentTypeStrategy}. */
class EqualsComparableElementArgumentTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "Array elements with equality", sequence(ARRAY_EQUALS_COMPARABLE))
                        .expectSignature("f(<ARRAY>)")
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT()))))
                        .expectArgumentTypes(
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT())))),
                TestSpec.forStrategy(
                                "Array elements without equality",
                                sequence(ARRAY_EQUALS_COMPARABLE))
                        .calledWithArgumentTypes(DataTypes.ARRAY(DataTypes.VARIANT()))
                        .expectErrorMessage(
                                "Array elements of type VARIANT cannot be compared, because the type has no equality. Cast the elements to a comparable type first."),
                TestSpec.forStrategy("No array", sequence(ARRAY_EQUALS_COMPARABLE))
                        .calledWithArgumentTypes(DataTypes.INT())
                        .expectErrorMessage(
                                "Invalid input arguments. Expected signatures are:\nf(<ARRAY>)"),
                TestSpec.forStrategy(
                                "Map keys with equality, values without",
                                sequence(MAP_KEYS_EQUALS_COMPARABLE))
                        .expectSignature("f(<MAP>)")
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.VARIANT()))
                        .expectArgumentTypes(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.VARIANT())),
                TestSpec.forStrategy(
                                "Map keys without equality", sequence(MAP_KEYS_EQUALS_COMPARABLE))
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.VARIANT(), DataTypes.INT()))
                        .expectErrorMessage(
                                "Map keys of type VARIANT cannot be compared, because the type has no equality. Cast the keys to a comparable type first."),
                TestSpec.forStrategy(
                                "Array elements of a structured type with a VARIANT attribute",
                                sequence(ARRAY_EQUALS_COMPARABLE))
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        new FieldsDataType(
                                                StructuredType.newBuilder(
                                                                ObjectIdentifier.of(
                                                                        "cat", "db", "type"))
                                                        .attributes(
                                                                List.of(
                                                                        new StructuredType
                                                                                .StructuredAttribute(
                                                                                "v",
                                                                                new VariantType())))
                                                        .build(),
                                                List.of(DataTypes.VARIANT()))))
                        .expectErrorMessage(
                                "Array elements of type `cat`.`db`.`type` cannot be compared, because the type has no equality. Cast the elements to a comparable type first."));
    }
}
