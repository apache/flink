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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link LogicalType} serialization and deserialization. */
@RunWith(Parameterized.class)
public class LogicalTypeSerdeTest {

    @Parameterized.Parameter public LogicalType logicalType;

    @Test
    public void testLogicalTypeSerde() throws IOException {
        SerdeContext serdeCtx =
                new SerdeContext(
                        new Configuration(), Thread.currentThread().getContextClassLoader());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();

        module.addSerializer(new LogicalTypeJsonSerializer());
        module.addSerializer(new ObjectIdentifierJsonSerializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(logicalType);
        }
        String json = writer.toString();
        LogicalType actual = mapper.readValue(json, LogicalType.class);
        assertEquals(logicalType, actual);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<LogicalType> testData() {
        List<LogicalType> types =
                Arrays.asList(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DecimalType(10),
                        new DecimalType(15, 5),
                        new CharType(),
                        new CharType(5),
                        new VarCharType(),
                        new VarCharType(5),
                        new BinaryType(),
                        new BinaryType(100),
                        new VarBinaryType(),
                        new VarBinaryType(100),
                        new DateType(),
                        new TimeType(),
                        new TimeType(3),
                        new TimestampType(),
                        new TimestampType(3),
                        new TimestampType(false, TimestampKind.PROCTIME, 3),
                        new TimestampType(false, TimestampKind.ROWTIME, 3),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR),
                        new DayTimeIntervalType(
                                false, DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR, 3, 6),
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                        new YearMonthIntervalType(
                                false, YearMonthIntervalType.YearMonthResolution.MONTH, 2),
                        new ZonedTimestampType(),
                        new ZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new SymbolType<>(TimeIntervalUnit.class),
                        new TypeInformationRawType<>(),
                        new TypeInformationRawType<>(Types.STRING),
                        new LegacyTypeInformationType<>(LogicalTypeRoot.RAW, Types.STRING),
                        new ArrayType(new IntType(false)),
                        new MapType(new BigIntType(), new IntType(false)),
                        new MultisetType(new IntType(false)),
                        RowType.of(new BigIntType(), new IntType(false), new VarCharType(200)),
                        RowType.of(
                                new LogicalType[] {
                                    new BigIntType(), new IntType(false), new VarCharType(200)
                                },
                                new String[] {"f1", "f2", "f3"}),
                        StructuredType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "structuredType"),
                                        PojoClass.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f0", new IntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new BigIntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f2", new VarCharType(200), "desc")))
                                .comparision(StructuredType.StructuredComparision.FULL)
                                .setFinal(false)
                                .setInstantiable(false)
                                .superType(
                                        StructuredType.newBuilder(
                                                        ObjectIdentifier.of(
                                                                "cat", "db", "structuredType2"))
                                                .attributes(
                                                        Collections.singletonList(
                                                                new StructuredType
                                                                        .StructuredAttribute(
                                                                        "f0",
                                                                        new BigIntType(false))))
                                                .build())
                                .description("description for StructuredType")
                                .build(),
                        DistinctType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "distinctType"),
                                        new VarCharType(5))
                                .build(),
                        DistinctType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "distinctType"),
                                        new VarCharType(false, 5))
                                .build(),
                        new RawType<>(
                                PojoClass.class,
                                new KryoSerializer<>(PojoClass.class, new ExecutionConfig())));
        List<LogicalType> newTypes = new ArrayList<>();
        // consider nullable
        for (LogicalType type : types) {
            newTypes.add(type.copy(true));
            newTypes.add(type.copy(false));
        }
        // ignore nullable for NullType
        newTypes.add(new NullType());
        return newTypes;
    }

    /** Testing class. */
    public static class PojoClass {
        private final int f0;
        private final long f1;
        private final String f2;

        public PojoClass(int f0, long f1, String f2) {
            this.f0 = f0;
            this.f1 = f1;
            this.f2 = f2;
        }
    }
}
