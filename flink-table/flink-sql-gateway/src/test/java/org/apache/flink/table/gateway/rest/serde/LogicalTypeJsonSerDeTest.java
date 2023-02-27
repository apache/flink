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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerdeTest;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
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
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link LogicalType} serialization and deserialization. */
@Execution(CONCURRENT)
class LogicalTypeJsonSerDeTest {

    private final ObjectMapper mapper = buildObjectMapper();

    @ParameterizedTest
    @MethodSource("generateTestData")
    void testLogicalTypeJsonSerDe(LogicalType logicalType) throws IOException {
        String json = mapper.writeValueAsString(logicalType);
        LogicalType actualType = mapper.readValue(json, LogicalType.class);

        assertThat(actualType).isEqualTo(logicalType);
    }

    @Test
    void testSerializeUnsupportedType() {
        LogicalType unsupportedType =
                StructuredType.newBuilder(DataTypeJsonSerdeTest.PojoClass.class)
                        .attributes(
                                Arrays.asList(
                                        new StructuredType.StructuredAttribute(
                                                "f0", new IntType(true)),
                                        new StructuredType.StructuredAttribute(
                                                "f1", new BigIntType(true)),
                                        new StructuredType.StructuredAttribute(
                                                "f2", new VarCharType(200), "desc")))
                        .build();
        assertThatThrownBy(() -> mapper.writeValueAsString(unsupportedType))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                UnsupportedOperationException.class,
                                String.format(
                                        "Unable to serialize logical type '%s'. Please check the documentation for supported types.",
                                        unsupportedType.asSummaryString())));
    }

    @Test
    void testDeserializeUnsupportedType() {
        String unsupportedTypeString = "STRUCTURED_TYPE";
        String json =
                String.format(
                        "{\"%s\": \"%s\", \"%s\": %s}",
                        "type", unsupportedTypeString, "nullable", "true");
        assertThatThrownBy(() -> mapper.readValue(json, LogicalType.class))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                UnsupportedOperationException.class,
                                String.format(
                                        "Unable to deserialize a logical type of type root '%s'. Please check the documentation for supported types.",
                                        unsupportedTypeString)));
    }

    @Test
    void testDeserializeUnsupportedJson() {
        String json = String.format("{\"%s\": \"%s\"}", "unknown", "whatever");
        assertThatThrownBy(() -> mapper.readValue(json, LogicalType.class))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                UnsupportedOperationException.class,
                                "Cannot parse this Json String"));
    }

    // --------------------------------------------------------------------------------------------
    // Test data
    // --------------------------------------------------------------------------------------------

    private static List<LogicalType> generateTestData() {
        List<LogicalType> types =
                Arrays.asList(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DateType(),
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
                        new DecimalType(10),
                        new DecimalType(15, 5),
                        new TimeType(),
                        new TimeType(3),
                        new TimestampType(),
                        new TimestampType(3),
                        new TimestampType(false, 3),
                        new ZonedTimestampType(),
                        new ZonedTimestampType(3),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(false, 3),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new MapType(new BigIntType(), new IntType(false)),
                        new MapType(CharType.ofEmptyLiteral(), CharType.ofEmptyLiteral()),
                        new MapType(VarCharType.ofEmptyLiteral(), VarCharType.ofEmptyLiteral()),
                        new MapType(BinaryType.ofEmptyLiteral(), BinaryType.ofEmptyLiteral()),
                        new MapType(VarBinaryType.ofEmptyLiteral(), VarBinaryType.ofEmptyLiteral()),
                        new MapType(new TimestampType(false, 3), new LocalZonedTimestampType()),
                        new ArrayType(new IntType(false)),
                        new ArrayType(new TimestampType()),
                        new ArrayType(new LocalZonedTimestampType(false, 3)),
                        new ArrayType(CharType.ofEmptyLiteral()),
                        new ArrayType(VarCharType.ofEmptyLiteral()),
                        new ArrayType(BinaryType.ofEmptyLiteral()),
                        new ArrayType(VarBinaryType.ofEmptyLiteral()),
                        new MultisetType(new IntType(false)),
                        new MultisetType(new TimestampType()),
                        new MultisetType(new TimestampType(true, 3)),
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
                                new TimestampType(false, 3), new LocalZonedTimestampType(false, 3)),
                        RowType.of(
                                CharType.ofEmptyLiteral(),
                                VarCharType.ofEmptyLiteral(),
                                BinaryType.ofEmptyLiteral(),
                                VarBinaryType.ofEmptyLiteral()),
                        // Row with descriptions
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("ID", new BigIntType(), "ID desc"),
                                        new RowType.RowField(
                                                "Name", new VarCharType(20), "Name desc"))),
                        // custom RawType
                        new RawType<>(LocalDateTime.class, LocalDateTimeSerializer.INSTANCE),
                        // external RawType
                        new RawType<>(
                                Row.class,
                                ExternalSerializer.of(
                                        DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()))),
                        // interval types
                        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.MONTH),
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.HOUR),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.MINUTE),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.SECOND));

        List<LogicalType> testTypes =
                Stream.concat(
                                types.stream().map(type -> type.copy(true)),
                                types.stream().map(type -> type.copy(false)))
                        .collect(Collectors.toList());

        // ignore nullable for NullType
        testTypes.add(new NullType());

        return testTypes;
    }

    private ObjectMapper buildObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(new LogicalTypeJsonSerializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        mapper.registerModule(module);
        return mapper;
    }

    // --------------------------------------------------------------------------------------------
    // Helper POJOs
    // --------------------------------------------------------------------------------------------

    /** Testing class. */
    public static class PojoClass {
        public int f0;
        public long f1;
        public String f2;
    }
}
