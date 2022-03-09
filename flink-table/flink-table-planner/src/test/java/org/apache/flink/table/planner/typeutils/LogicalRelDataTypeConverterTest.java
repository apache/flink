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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.test.TableAssertions.assertThat;

/** Tests for {@link LogicalRelDataTypeConverter}. */
public class LogicalRelDataTypeConverterTest {

    @ParameterizedTest
    @MethodSource("testConversion")
    public void testConversion(LogicalType logicalType) throws IOException {
        final RelDataTypeFactory typeFactory = FlinkTypeFactory.INSTANCE();
        final DataTypeFactoryMock dataTypeFactory = new DataTypeFactoryMock();
        final RelDataType relDataType =
                LogicalRelDataTypeConverter.toRelDataType(logicalType, typeFactory);
        assertThat(LogicalRelDataTypeConverter.toLogicalType(relDataType, dataTypeFactory))
                .isEqualTo(logicalType);
    }

    // --------------------------------------------------------------------------------------------
    // Test data
    // --------------------------------------------------------------------------------------------

    private static Stream<LogicalType> testConversion() {
        return Stream.of(
                new BooleanType(),
                new TinyIntType(),
                new SmallIntType(),
                new IntType(),
                new BigIntType(),
                new FloatType(),
                new DoubleType(),
                new DecimalType(10),
                new DecimalType(15, 5),
                CharType.ofEmptyLiteral(),
                new CharType(),
                new CharType(5),
                VarCharType.ofEmptyLiteral(),
                new VarCharType(),
                new VarCharType(5),
                BinaryType.ofEmptyLiteral(),
                new BinaryType(),
                new BinaryType(100),
                VarBinaryType.ofEmptyLiteral(),
                new VarBinaryType(),
                new VarBinaryType(100),
                new DateType(),
                new TimeType(),
                new TimeType(3),
                new TimestampType(),
                new TimestampType(3),
                new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                new TimestampType(false, TimestampKind.ROWTIME, 3),
                new LocalZonedTimestampType(),
                new LocalZonedTimestampType(3),
                new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR),
                new DayTimeIntervalType(
                        false, DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR, 3, 6),
                new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                new YearMonthIntervalType(
                        false, YearMonthIntervalType.YearMonthResolution.MONTH, 2),
                new LocalZonedTimestampType(),
                new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                new SymbolType<>(),
                new ArrayType(new IntType(false)),
                new ArrayType(new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3)),
                new ArrayType(new TimestampType()),
                new ArrayType(CharType.ofEmptyLiteral()),
                new ArrayType(VarCharType.ofEmptyLiteral()),
                new ArrayType(BinaryType.ofEmptyLiteral()),
                new ArrayType(VarBinaryType.ofEmptyLiteral()),
                new MapType(new BigIntType(), new IntType(false)),
                new MapType(
                        new TimestampType(false, TimestampKind.ROWTIME, 3),
                        new LocalZonedTimestampType()),
                new MapType(CharType.ofEmptyLiteral(), CharType.ofEmptyLiteral()),
                new MapType(VarCharType.ofEmptyLiteral(), VarCharType.ofEmptyLiteral()),
                new MapType(BinaryType.ofEmptyLiteral(), BinaryType.ofEmptyLiteral()),
                new MapType(VarBinaryType.ofEmptyLiteral(), VarBinaryType.ofEmptyLiteral()),
                new MultisetType(new IntType(false)),
                new MultisetType(new TimestampType()),
                new MultisetType(new TimestampType(true, TimestampKind.ROWTIME, 3)),
                new MultisetType(CharType.ofEmptyLiteral()),
                new MultisetType(VarCharType.ofEmptyLiteral()),
                new MultisetType(BinaryType.ofEmptyLiteral()),
                new MultisetType(VarBinaryType.ofEmptyLiteral()),
                RowType.of(new BigIntType(), new IntType(false), new VarCharType(200)),
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(), new IntType(false), new VarCharType(200)
                        },
                        new String[] {"f1", "f2", "f3"}),
                RowType.of(
                        new TimestampType(false, TimestampKind.ROWTIME, 3),
                        new TimestampType(false, TimestampKind.REGULAR, 3),
                        new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new LocalZonedTimestampType(false, TimestampKind.REGULAR, 3)),
                RowType.of(
                        CharType.ofEmptyLiteral(),
                        VarCharType.ofEmptyLiteral(),
                        BinaryType.ofEmptyLiteral(),
                        VarBinaryType.ofEmptyLiteral()),
                // registered structured type
                PojoClass.TYPE_WITH_IDENTIFIER,
                // unregistered structured type
                StructuredType.newBuilder(PojoClass.class)
                        .attributes(
                                Arrays.asList(
                                        new StructuredType.StructuredAttribute(
                                                "f0", new IntType(true)),
                                        new StructuredType.StructuredAttribute(
                                                "f1", new BigIntType(true)),
                                        new StructuredType.StructuredAttribute(
                                                "f2", new VarCharType(200), "desc")))
                        .build(),
                // custom RawType
                new RawType<>(LocalDateTime.class, LocalDateTimeSerializer.INSTANCE));
    }

    // --------------------------------------------------------------------------------------------
    // Shared utilities
    // --------------------------------------------------------------------------------------------

    /** Testing class. */
    public static class PojoClass {

        public static final LogicalType TYPE_WITH_IDENTIFIER =
                StructuredType.newBuilder(
                                ObjectIdentifier.of("cat", "db", "structuredType"), PojoClass.class)
                        .attributes(
                                Arrays.asList(
                                        new StructuredType.StructuredAttribute(
                                                "f0", new IntType(true)),
                                        new StructuredType.StructuredAttribute(
                                                "f1", new BigIntType(true)),
                                        new StructuredType.StructuredAttribute(
                                                "f2", new VarCharType(200), "desc")))
                        .comparison(StructuredType.StructuredComparison.FULL)
                        .setFinal(false)
                        .setInstantiable(false)
                        .superType(
                                StructuredType.newBuilder(
                                                ObjectIdentifier.of("cat", "db", "structuredType2"))
                                        .attributes(
                                                Collections.singletonList(
                                                        new StructuredType.StructuredAttribute(
                                                                "f0", new BigIntType(false))))
                                        .build())
                        .description("description for StructuredType")
                        .build();

        public int f0;
        public long f1;
        public String f2;
    }
}
