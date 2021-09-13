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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Tests for serialization/deserialization of {@link RelDataType}. */
@RunWith(Parameterized.class)
public class RelDataTypeSerdeTest {
    private static final FlinkTypeFactory FACTORY = FlinkTypeFactory.INSTANCE();

    @Parameterized.Parameters(name = "type = {0}")
    public static Collection<RelDataType> parameters() {
        // the values in the list do not care about nullable.
        List<RelDataType> types =
                Arrays.asList(
                        FACTORY.createSqlType(SqlTypeName.BOOLEAN),
                        FACTORY.createSqlType(SqlTypeName.TINYINT),
                        FACTORY.createSqlType(SqlTypeName.SMALLINT),
                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                        FACTORY.createSqlType(SqlTypeName.BIGINT),
                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 3, 10),
                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 0, 19),
                        FACTORY.createSqlType(SqlTypeName.DECIMAL, -1, 19),
                        FACTORY.createSqlType(SqlTypeName.FLOAT),
                        FACTORY.createSqlType(SqlTypeName.REAL),
                        FACTORY.createSqlType(SqlTypeName.DOUBLE),
                        FACTORY.createSqlType(SqlTypeName.DATE),
                        FACTORY.createSqlType(SqlTypeName.TIME),
                        FACTORY.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE),
                        FACTORY.createSqlType(SqlTypeName.TIMESTAMP),
                        FACTORY.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.YEAR, TimeUnit.YEAR, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.MONTH, TimeUnit.MONTH, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.DAY, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.HOUR, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.HOUR, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.HOUR, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE, TimeUnit.MINUTE, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.MINUTE, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        FACTORY.createSqlIntervalType(
                                new SqlIntervalQualifier(
                                        TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO)),
                        FACTORY.createSqlType(SqlTypeName.CHAR),
                        FACTORY.createSqlType(SqlTypeName.CHAR, 0),
                        FACTORY.createSqlType(SqlTypeName.CHAR, 32),
                        FACTORY.createSqlType(SqlTypeName.VARCHAR),
                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 0),
                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 10),
                        FACTORY.createSqlType(SqlTypeName.BINARY),
                        FACTORY.createSqlType(SqlTypeName.BINARY, 0),
                        FACTORY.createSqlType(SqlTypeName.BINARY, 100),
                        FACTORY.createSqlType(SqlTypeName.VARBINARY),
                        FACTORY.createSqlType(SqlTypeName.VARBINARY, 0),
                        FACTORY.createSqlType(SqlTypeName.VARBINARY, 1000),
                        FACTORY.createSqlType(SqlTypeName.NULL),
                        FACTORY.createSqlType(SqlTypeName.ANY),
                        FACTORY.createSqlType(SqlTypeName.SYMBOL),
                        FACTORY.createMultisetType(FACTORY.createSqlType(SqlTypeName.VARCHAR), -1),
                        FACTORY.createArrayType(FACTORY.createSqlType(SqlTypeName.VARCHAR, 16), -1),
                        FACTORY.createArrayType(
                                FACTORY.createArrayType(
                                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 16), -1),
                                -1),
                        FACTORY.createMapType(
                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 10)),
                        FACTORY.createMapType(
                                FACTORY.createMapType(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                                        FACTORY.createSqlType(SqlTypeName.VARCHAR, 10)),
                                FACTORY.createArrayType(
                                        FACTORY.createMapType(
                                                FACTORY.createSqlType(SqlTypeName.INTEGER),
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 10)),
                                        -1)),
                        FACTORY.createSqlType(SqlTypeName.DISTINCT),
                        FACTORY.createSqlType(SqlTypeName.STRUCTURED),
                        // simple struct type
                        FACTORY.createStructType(
                                StructKind.PEEK_FIELDS,
                                Arrays.asList(
                                        FACTORY.createSqlType(SqlTypeName.INTEGER),
                                        FACTORY.createSqlType(SqlTypeName.DECIMAL, 3, 10)),
                                Arrays.asList("f1", "f2")),
                        // struct type with array type
                        FACTORY.createStructType(
                                Arrays.asList(
                                        FACTORY.createSqlType(SqlTypeName.VARCHAR),
                                        FACTORY.createArrayType(
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 16),
                                                -1)),
                                Arrays.asList("f1", "f2")),
                        // nested struct type
                        FACTORY.createStructType(
                                Arrays.asList(
                                        FACTORY.createStructType(
                                                Arrays.asList(
                                                        FACTORY.createSqlType(
                                                                SqlTypeName.VARCHAR, 5),
                                                        FACTORY.createSqlType(
                                                                SqlTypeName.VARCHAR, 10)),
                                                Arrays.asList("f1", "f2")),
                                        FACTORY.createArrayType(
                                                FACTORY.createSqlType(SqlTypeName.VARCHAR, 16),
                                                -1)),
                                Arrays.asList("f3", "f4")),
                        FACTORY.createSqlType(SqlTypeName.SARG),
                        FACTORY.createRowtimeIndicatorType(true, false),
                        FACTORY.createRowtimeIndicatorType(true, true),
                        FACTORY.createProctimeIndicatorType(true),
                        FACTORY.createFieldTypeFromLogicalType(
                                new LegacyTypeInformationType<>(LogicalTypeRoot.RAW, Types.STRING)),
                        FACTORY.createFieldTypeFromLogicalType(
                                StructuredType.newBuilder(
                                                ObjectIdentifier.of("cat", "db", "structuredType"),
                                                LogicalTypeSerdeTest.PojoClass.class)
                                        .attributes(
                                                Arrays.asList(
                                                        new StructuredType.StructuredAttribute(
                                                                "f0", new IntType(true)),
                                                        new StructuredType.StructuredAttribute(
                                                                "f1", new BigIntType(true)),
                                                        new StructuredType.StructuredAttribute(
                                                                "f2",
                                                                new VarCharType(200),
                                                                "desc")))
                                        .comparison(StructuredType.StructuredComparison.FULL)
                                        .setFinal(false)
                                        .setInstantiable(false)
                                        .description("description for StructuredType")
                                        .build()));

        List<RelDataType> ret = new ArrayList<>(types.size() * 2);
        for (RelDataType type : types) {
            ret.add(FACTORY.createTypeWithNullability(type, true));
            ret.add(FACTORY.createTypeWithNullability(type, false));
        }

        ret.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new RawType<>(true, Void.class, VoidSerializer.INSTANCE)),
                        true));
        ret.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new RawType<>(false, Void.class, VoidSerializer.INSTANCE)),
                        false));
        ret.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new RawType<>(true, Void.class, VoidSerializer.INSTANCE)),
                        false));
        ret.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new TypeInformationRawType<>(true, Types.STRING)),
                        true));
        ret.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new TypeInformationRawType<>(false, Types.STRING)),
                        false));
        ret.add(
                FACTORY.createTypeWithNullability(
                        FACTORY.createFieldTypeFromLogicalType(
                                new TypeInformationRawType<>(true, Types.STRING)),
                        false));

        return ret;
    }

    @Parameterized.Parameter public RelDataType relDataType;

    @Test
    public void testTypeSerde() throws Exception {
        SerdeContext serdeCtx =
                new SerdeContext(
                        new FlinkContextImpl(false, TableConfig.getDefault(), null, null, null),
                        Thread.currentThread().getContextClassLoader(),
                        FlinkTypeFactory.INSTANCE(),
                        FlinkSqlOperatorTable.instance());
        ObjectMapper mapper = JsonSerdeUtil.createObjectMapper(serdeCtx);
        SimpleModule module = new SimpleModule();

        module.addSerializer(new RelDataTypeJsonSerializer());
        module.addSerializer(new LogicalTypeJsonSerializer());
        module.addSerializer(new ObjectIdentifierJsonSerializer());
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        module.addDeserializer(ObjectIdentifier.class, new ObjectIdentifierJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(relDataType);
        }

        String json = writer.toString();
        RelDataType actual = mapper.readValue(json, RelDataType.class);
        // type system will fill the default precision if the precision is not defined
        if (relDataType.toString().equals("DECIMAL")) {
            assertEquals(SqlTypeName.DECIMAL, actual.getSqlTypeName());
            assertEquals(relDataType.getScale(), actual.getScale());
            assertEquals(
                    serdeCtx.getTypeFactory()
                            .getTypeSystem()
                            .getDefaultPrecision(SqlTypeName.DECIMAL),
                    actual.getPrecision());
        } else {
            assertSame(relDataType, actual);
        }
    }
}
