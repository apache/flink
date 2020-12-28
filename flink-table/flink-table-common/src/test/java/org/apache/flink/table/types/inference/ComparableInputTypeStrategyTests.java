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

package org.apache.flink.table.types.inference;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.inference.strategies.ComparableTypeStrategy;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparision;

import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/** Tests for {@link ComparableTypeStrategy}. */
public class ComparableInputTypeStrategyTests extends InputTypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return asList(
                TestSpec.forStrategy(
                                "Numeric types are comparable",
                                InputTypeStrategies.comparable(
                                        ConstantArgumentCount.of(7), StructuredComparision.EQUALS))
                        .calledWithArgumentTypes(
                                DataTypes.TINYINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.DOUBLE(),
                                DataTypes.FLOAT(),
                                DataTypes.DECIMAL(10, 2))
                        .expectSignature("f(<COMPARABLE>...)")
                        .expectArgumentTypes(
                                DataTypes.TINYINT(),
                                DataTypes.SMALLINT(),
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.DOUBLE(),
                                DataTypes.FLOAT(),
                                DataTypes.DECIMAL(10, 2)),
                TestSpec.forStrategy(
                                "Datetime types are comparable",
                                InputTypeStrategies.comparable(
                                        ConstantArgumentCount.of(4), StructuredComparision.EQUALS))
                        .calledWithArgumentTypes(
                                DataTypes.TIMESTAMP(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                                DataTypes.TIMESTAMP_WITH_TIME_ZONE(),
                                DataTypes.DATE())
                        .expectArgumentTypes(
                                DataTypes.TIMESTAMP(),
                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
                                DataTypes.TIMESTAMP_WITH_TIME_ZONE(),
                                DataTypes.DATE()),
                TestSpec.forStrategy(
                                "VARCHAR and CHAR types are comparable",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(DataTypes.VARCHAR(10), DataTypes.CHAR(13))
                        .expectArgumentTypes(DataTypes.VARCHAR(10), DataTypes.CHAR(13)),
                TestSpec.forStrategy(
                                "VARBINARY and BINARY types are comparable",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(DataTypes.VARBINARY(10), DataTypes.BINARY(13))
                        .expectArgumentTypes(DataTypes.VARBINARY(10), DataTypes.BINARY(13)),
                TestSpec.forStrategy(
                                "Comparable array types", InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.TINYINT()),
                                DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)))
                        .expectArgumentTypes(
                                DataTypes.ARRAY(DataTypes.TINYINT()),
                                DataTypes.ARRAY(DataTypes.DECIMAL(10, 2))),
                TestSpec.forStrategy(
                                "Comparable map types", InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.TINYINT(), DataTypes.TIMESTAMP()),
                                DataTypes.MAP(
                                        DataTypes.DECIMAL(10, 3),
                                        DataTypes.TIMESTAMP_WITH_TIME_ZONE()))
                        .expectArgumentTypes(
                                DataTypes.MAP(DataTypes.TINYINT(), DataTypes.TIMESTAMP()),
                                DataTypes.MAP(
                                        DataTypes.DECIMAL(10, 3),
                                        DataTypes.TIMESTAMP_WITH_TIME_ZONE())),
                TestSpec.forStrategy(
                                "Fully comparable structured types",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                structuredType(
                                                "type",
                                                singletonList(DataTypes.INT()),
                                                StructuredComparision.FULL)
                                        .notNull(),
                                structuredType(
                                                "type",
                                                singletonList(DataTypes.INT()),
                                                StructuredComparision.FULL)
                                        .nullable())
                        .expectArgumentTypes(
                                structuredType(
                                                "type",
                                                singletonList(DataTypes.INT()),
                                                StructuredComparision.FULL)
                                        .notNull(),
                                structuredType(
                                                "type",
                                                singletonList(DataTypes.INT()),
                                                StructuredComparision.FULL)
                                        .nullable()),
                TestSpec.forStrategy(
                                "Equals comparable structured types",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                structuredType(
                                        "type",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS),
                                structuredType(
                                        "type",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS))
                        .expectArgumentTypes(
                                structuredType(
                                        "type",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS),
                                structuredType(
                                        "type",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS)),
                TestSpec.forStrategy(
                                "Comparable arrays of structured types",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(
                                        structuredType(
                                                        "type",
                                                        singletonList(DataTypes.INT()),
                                                        StructuredComparision.EQUALS)
                                                .notNull()),
                                DataTypes.ARRAY(
                                        structuredType(
                                                        "type",
                                                        singletonList(DataTypes.INT()),
                                                        StructuredComparision.EQUALS)
                                                .nullable()))
                        .expectArgumentTypes(
                                DataTypes.ARRAY(
                                        structuredType(
                                                        "type",
                                                        singletonList(DataTypes.INT()),
                                                        StructuredComparision.EQUALS)
                                                .notNull()),
                                DataTypes.ARRAY(
                                        structuredType(
                                                        "type",
                                                        singletonList(DataTypes.INT()),
                                                        StructuredComparision.EQUALS)
                                                .nullable())),
                TestSpec.forStrategy(
                                "Distinct types are comparable if the source type is comparable",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                distinctType("type", DataTypes.INT()).notNull(),
                                distinctType("type", DataTypes.INT()).nullable())
                        .expectArgumentTypes(
                                distinctType("type", DataTypes.INT()).notNull(),
                                distinctType("type", DataTypes.INT()).nullable()),
                TestSpec.forStrategy(
                                "Comparable multisets of distinct types",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.MULTISET(distinctType("type", DataTypes.INT()).notNull()),
                                DataTypes.MULTISET(
                                        distinctType("type", DataTypes.INT()).nullable()))
                        .expectArgumentTypes(
                                DataTypes.MULTISET(distinctType("type", DataTypes.INT()).notNull()),
                                DataTypes.MULTISET(
                                        distinctType("type", DataTypes.INT()).nullable())),
                TestSpec.forStrategy(
                                "Everything is comparable with null type",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(DataTypes.INT(), DataTypes.NULL())
                        .expectArgumentTypes(DataTypes.INT(), DataTypes.NULL()),
                TestSpec.forStrategy(
                                "RAW types are comparable if the originating class implements Comparable",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                rawType(ComparableClass.class).notNull(),
                                rawType(ComparableClass.class).nullable())
                        .expectArgumentTypes(
                                rawType(ComparableClass.class).notNull(),
                                rawType(ComparableClass.class).nullable()),
                TestSpec.forStrategy(
                                "Comparable map of raw types",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.MAP(
                                        rawType(ComparableClass.class).notNull(),
                                        rawType(ComparableClass.class)),
                                DataTypes.MAP(
                                        rawType(ComparableClass.class).nullable(),
                                        rawType(ComparableClass.class)))
                        .expectArgumentTypes(
                                DataTypes.MAP(
                                        rawType(ComparableClass.class).notNull(),
                                        rawType(ComparableClass.class)),
                                DataTypes.MAP(
                                        rawType(ComparableClass.class).nullable(),
                                        rawType(ComparableClass.class))),
                TestSpec.forStrategy(
                                "RAW types are not comparable if the originating class does not implement Comparable",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                rawType(NotComparableClass.class),
                                rawType(NotComparableClass.class))
                        .expectErrorMessage(
                                String.format(
                                        "All types in a comparison should support 'EQUALS' comparison with"
                                                + " each other. Can not compare RAW('%s', '...') with RAW('%s', '...')",
                                        NotComparableClass.class.getName(),
                                        NotComparableClass.class.getName())),
                TestSpec.forStrategy(
                                "RAW types are not comparable if the types are different",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                rawType(NotComparableClass.class),
                                DataTypes.RAW(TypeInformation.of(NotComparableClass.class)))
                        .expectErrorMessage(
                                String.format(
                                        "All types in a comparison should support 'EQUALS' comparison with"
                                                + " each other. Can not compare RAW('%s', '...') with RAW('%s', ?)",
                                        NotComparableClass.class.getName(),
                                        NotComparableClass.class.getName())),
                TestSpec.forStrategy(
                                "Not fully comparable structured types",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                structuredType(
                                        "type",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS),
                                structuredType(
                                        "type",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS))
                        .expectErrorMessage(
                                "All types in a comparison should support both 'EQUALS' and 'ORDER' comparison"
                                        + " with each other. Can not compare `cat`.`db`.`type` with `cat`.`db`.`type`"),
                TestSpec.forStrategy(
                                "Two different structured types are not comparable",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                structuredType(
                                        "type1",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS),
                                structuredType(
                                        "type2",
                                        singletonList(DataTypes.INT()),
                                        StructuredComparision.EQUALS))
                        .expectErrorMessage(
                                "All types in a comparison should support 'EQUALS' comparison with each other."
                                        + " Can not compare `cat`.`db`.`type1` with `cat`.`db`.`type2`"),
                TestSpec.forStrategy(
                                "Two different different distinct types are not comparable even if point to the same type",
                                InputTypeStrategies.TWO_EQUALS_COMPARABLE)
                        .calledWithArgumentTypes(
                                distinctType("type1", DataTypes.INT()),
                                distinctType("type2", DataTypes.INT()))
                        .expectErrorMessage(
                                "All types in a comparison should support 'EQUALS' comparison with each other."
                                        + " Can not compare `cat`.`db`.`type1` with `cat`.`db`.`type2`"),
                TestSpec.forStrategy(
                                "Not comparable array types",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.ARRAY(DataTypes.TINYINT()),
                                DataTypes.ARRAY(DataTypes.VARCHAR(2)))
                        .expectErrorMessage(
                                "All types in a comparison should support both 'EQUALS' and 'ORDER' comparison"
                                        + " with each other. Can not compare ARRAY<TINYINT> with ARRAY<VARCHAR(2)>"),
                TestSpec.forStrategy(
                                "Not comparable key types in map types",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.TINYINT(), DataTypes.TIMESTAMP()),
                                DataTypes.MAP(
                                        DataTypes.VARCHAR(3), DataTypes.TIMESTAMP_WITH_TIME_ZONE()))
                        .expectErrorMessage(
                                "All types in a comparison should support both 'EQUALS' and 'ORDER' comparison"
                                        + " with each other. Can not compare MAP<TINYINT, TIMESTAMP(6)> with"
                                        + " MAP<VARCHAR(3), TIMESTAMP(6) WITH TIME ZONE>"),
                TestSpec.forStrategy(
                                "Not comparable value types in map types",
                                InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(
                                DataTypes.MAP(DataTypes.TINYINT(), DataTypes.TIMESTAMP()),
                                DataTypes.MAP(DataTypes.DECIMAL(10, 3), DataTypes.INT()))
                        .expectErrorMessage(
                                "All types in a comparison should support both 'EQUALS' and 'ORDER' comparison"
                                        + " with each other. Can not compare MAP<TINYINT, TIMESTAMP(6)> with MAP<DECIMAL(10, 3), INT>"),
                TestSpec.forStrategy(
                                "Not comparable types", InputTypeStrategies.TWO_FULLY_COMPARABLE)
                        .calledWithArgumentTypes(DataTypes.TIMESTAMP(), DataTypes.BIGINT())
                        .expectErrorMessage(
                                "All types in a comparison should support both 'EQUALS' and 'ORDER' comparison"
                                        + " with each other. Can not compare TIMESTAMP(6) with BIGINT"));
    }

    private static <T> DataType rawType(Class<T> clazz) {
        return DataTypes.RAW(clazz, new KryoSerializer<>(clazz, new ExecutionConfig()));
    }

    private static DataType distinctType(String typeName, DataType sourceType) {
        return new AtomicDataType(
                DistinctType.newBuilder(
                                ObjectIdentifier.of("cat", "db", typeName),
                                sourceType.getLogicalType())
                        .build(),
                sourceType.getConversionClass());
    }

    private static DataType structuredType(
            String typeName, List<DataType> fieldDataTypes, StructuredComparision comparision) {
        return new FieldsDataType(
                StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", typeName))
                        .attributes(
                                IntStream.range(0, fieldDataTypes.size())
                                        .mapToObj(
                                                idx ->
                                                        new StructuredType.StructuredAttribute(
                                                                "f" + idx,
                                                                fieldDataTypes
                                                                        .get(idx)
                                                                        .getLogicalType()))
                                        .collect(Collectors.toList()))
                        .comparision(comparision)
                        .build(),
                fieldDataTypes);
    }

    private static class ComparableClass implements Comparable<ComparableClass> {
        @Override
        public int compareTo(@Nonnull ComparableClass o) {
            return 0;
        }
    }

    private static class NotComparableClass {}
}
