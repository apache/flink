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

package org.apache.flink.table.catalog;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ExternalSchemaTranslator.InputResult;
import org.apache.flink.table.catalog.ExternalSchemaTranslator.OutputResult;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ExternalSchemaTranslator}. */
public class ExternalSchemaTranslatorTest {

    @Test
    public void testInputFromRow() {
        final TypeInformation<?> inputTypeInfo =
                Types.ROW(Types.ROW(Types.INT, Types.BOOLEAN), Types.ENUM(DayOfWeek.class));

        final InputResult result =
                ExternalSchemaTranslator.fromExternal(
                        dataTypeFactoryWithRawType(DayOfWeek.class), inputTypeInfo, null);

        assertEquals(
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "f0",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.INT()),
                                                DataTypes.FIELD("f1", DataTypes.BOOLEAN()))),
                                DataTypes.FIELD(
                                        "f1", DataTypeFactoryMock.dummyRaw(DayOfWeek.class)))
                        .notNull(),
                result.getPhysicalDataType());

        assertTrue(result.isTopLevelRecord());

        assertEquals(
                Schema.newBuilder()
                        .column(
                                "f0",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f0", DataTypes.INT()),
                                        DataTypes.FIELD("f1", DataTypes.BOOLEAN())))
                        .column("f1", DataTypeFactoryMock.dummyRaw(DayOfWeek.class))
                        .build(),
                result.getSchema());

        assertNull(result.getProjections());
    }

    @Test
    public void testOutputToRowDataType() {
        final ResolvedSchema inputSchema =
                ResolvedSchema.of(
                        Column.physical("c", DataTypes.INT()),
                        Column.physical("a", DataTypes.BOOLEAN()),
                        Column.physical("b", DataTypes.DOUBLE()));

        final DataType physicalDataType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("b", DataTypes.DOUBLE()),
                        DataTypes.FIELD("c", DataTypes.INT()));

        final OutputResult result =
                ExternalSchemaTranslator.fromInternal(
                        dataTypeFactory(), inputSchema, physicalDataType);

        assertEquals(Optional.of(Arrays.asList("a", "b", "c")), result.getProjections());

        assertEquals(
                Schema.newBuilder()
                        .column("a", DataTypes.BOOLEAN())
                        .column("b", DataTypes.DOUBLE())
                        .column("c", DataTypes.INT())
                        .build(),
                result.getSchema());

        assertEquals(Optional.of(physicalDataType), result.getPhysicalDataType());
    }

    @Test
    public void testInputFromAtomic() {
        final TypeInformation<?> inputTypeInfo = Types.GENERIC(Row.class);

        final InputResult result =
                ExternalSchemaTranslator.fromExternal(
                        dataTypeFactoryWithRawType(Row.class), inputTypeInfo, null);

        assertEquals(DataTypeFactoryMock.dummyRaw(Row.class), result.getPhysicalDataType());

        assertFalse(result.isTopLevelRecord());

        assertEquals(
                Schema.newBuilder().column("f0", DataTypeFactoryMock.dummyRaw(Row.class)).build(),
                result.getSchema());

        assertNull(result.getProjections());
    }

    @Test
    public void testOutputToAtomicDataType() {
        final ResolvedSchema inputSchema = ResolvedSchema.of(Column.physical("a", DataTypes.INT()));

        final OutputResult result =
                ExternalSchemaTranslator.fromInternal(
                        dataTypeFactory(), inputSchema, DataTypes.INT());

        assertEquals(Optional.empty(), result.getProjections());

        assertEquals(Schema.newBuilder().column("f0", DataTypes.INT()).build(), result.getSchema());

        assertEquals(Optional.of(DataTypes.INT()), result.getPhysicalDataType());
    }

    @Test
    public void testInputFromRowWithNonPhysicalDeclaredSchema() {
        final TypeInformation<?> inputTypeInfo = Types.ROW(Types.INT, Types.LONG);

        final InputResult result =
                ExternalSchemaTranslator.fromExternal(
                        dataTypeFactory(),
                        inputTypeInfo,
                        Schema.newBuilder()
                                .columnByExpression("computed", "f1 + 42")
                                .columnByExpression("computed2", "f1 - 1")
                                .primaryKeyNamed("pk", "f0")
                                .build());

        assertEquals(
                DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.INT()),
                                DataTypes.FIELD("f1", DataTypes.BIGINT()))
                        .notNull(),
                result.getPhysicalDataType());

        assertTrue(result.isTopLevelRecord());

        assertEquals(
                Schema.newBuilder()
                        .column("f0", DataTypes.INT().notNull()) // not null due to primary key
                        .column("f1", DataTypes.BIGINT())
                        .columnByExpression("computed", "f1 + 42")
                        .columnByExpression("computed2", "f1 - 1")
                        .primaryKeyNamed("pk", "f0")
                        .build(),
                result.getSchema());

        assertNull(result.getProjections());
    }

    @Test
    public void testInputFromRowWithPhysicalDeclaredSchema() {
        final TypeInformation<?> inputTypeInfo =
                Types.ROW(Types.INT, Types.LONG, Types.GENERIC(BigDecimal.class), Types.BOOLEAN);

        final InputResult result =
                ExternalSchemaTranslator.fromExternal(
                        dataTypeFactoryWithRawType(BigDecimal.class),
                        inputTypeInfo,
                        Schema.newBuilder()
                                .primaryKeyNamed("pk", "f0")
                                .column("f1", DataTypes.BIGINT()) // reordered
                                .column("f0", DataTypes.INT())
                                .columnByExpression("computed", "f1 + 42")
                                .column("f2", DataTypes.DECIMAL(10, 2)) // enriches
                                .columnByExpression("computed2", "f1 - 1")
                                .build());

        assertEquals(
                DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.INT()),
                                DataTypes.FIELD("f1", DataTypes.BIGINT()),
                                DataTypes.FIELD("f2", DataTypes.DECIMAL(10, 2)),
                                DataTypes.FIELD("f3", DataTypes.BOOLEAN()))
                        .notNull(),
                result.getPhysicalDataType());

        assertTrue(result.isTopLevelRecord());

        assertEquals(
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.BIGINT())
                        .column("f2", DataTypes.DECIMAL(10, 2))
                        .column("f3", DataTypes.BOOLEAN())
                        .columnByExpression("computed", "f1 + 42")
                        .columnByExpression("computed2", "f1 - 1")
                        .primaryKeyNamed("pk", "f0")
                        .build(),
                result.getSchema());

        assertEquals(
                Arrays.asList("f1", "f0", "computed", "f2", "computed2"), result.getProjections());
    }

    @Test
    public void testInputFromAtomicWithPhysicalDeclaredSchema() {
        final TypeInformation<?> inputTypeInfo = Types.GENERIC(Row.class);

        final InputResult result =
                ExternalSchemaTranslator.fromExternal(
                        dataTypeFactoryWithRawType(Row.class),
                        inputTypeInfo,
                        Schema.newBuilder()
                                .columnByExpression("f0_0", "f0.f0_0")
                                .column(
                                        "f0",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("f0_0", DataTypes.INT()),
                                                DataTypes.FIELD("f0_1", DataTypes.BOOLEAN())))
                                .columnByExpression("f0_1", "f0.f0_1")
                                .build());

        assertEquals(
                DataTypes.ROW(
                        DataTypes.FIELD("f0_0", DataTypes.INT()),
                        DataTypes.FIELD("f0_1", DataTypes.BOOLEAN())),
                result.getPhysicalDataType());

        assertFalse(result.isTopLevelRecord());

        assertEquals(
                Schema.newBuilder()
                        .column(
                                "f0",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f0_0", DataTypes.INT()),
                                        DataTypes.FIELD("f0_1", DataTypes.BOOLEAN())))
                        .columnByExpression("f0_0", "f0.f0_0")
                        .columnByExpression("f0_1", "f0.f0_1")
                        .build(),
                result.getSchema());

        assertEquals(Arrays.asList("f0_0", "f0", "f0_1"), result.getProjections());
    }

    @Test
    public void testInvalidDeclaredSchemaColumn() {
        final TypeInformation<?> inputTypeInfo = Types.ROW(Types.INT, Types.LONG);

        try {
            ExternalSchemaTranslator.fromExternal(
                    dataTypeFactory(),
                    inputTypeInfo,
                    Schema.newBuilder().column("INVALID", DataTypes.BIGINT()).build());
        } catch (ValidationException e) {
            assertThat(
                    e,
                    containsMessage(
                            "Unable to find a field named 'INVALID' in the physical data type"));
        }
    }

    @Test
    public void testOutputToNoSchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.metadata("rowtime", DataTypes.TIMESTAMP_LTZ(3), null, false),
                        Column.physical("name", DataTypes.STRING()));

        final OutputResult result = ExternalSchemaTranslator.fromInternal(tableSchema, null);

        assertEquals(Optional.empty(), result.getProjections());

        assertEquals(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("rowtime", DataTypes.TIMESTAMP_LTZ(3)) // becomes physical
                        .column("name", DataTypes.STRING())
                        .build(),
                result.getSchema());

        assertEquals(Optional.empty(), result.getPhysicalDataType());
    }

    @Test
    public void testOutputToPartialSchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT().notNull()),
                        Column.physical("name", DataTypes.STRING()),
                        Column.metadata("rowtime", DataTypes.TIMESTAMP_LTZ(3), null, false));

        final OutputResult result =
                ExternalSchemaTranslator.fromInternal(
                        tableSchema,
                        Schema.newBuilder()
                                .columnByExpression("computed", "f1 + 42")
                                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                                .primaryKey("id")
                                .build());

        assertEquals(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("name", DataTypes.STRING())
                        .columnByExpression("computed", "f1 + 42")
                        .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3)) // becomes metadata
                        .primaryKey("id")
                        .build(),
                result.getSchema());
    }

    @Test
    public void testOutputToDeclaredSchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.BIGINT()),
                        Column.physical("rowtime", DataTypes.TIMESTAMP_LTZ(3)),
                        Column.physical("name", DataTypes.STRING()));

        final OutputResult result =
                ExternalSchemaTranslator.fromInternal(
                        tableSchema,
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT())
                                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                                .column("name", DataTypes.STRING().bridgedTo(StringData.class))
                                .build());

        assertEquals(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                        .column("name", DataTypes.STRING().bridgedTo(StringData.class))
                        .build(),
                result.getSchema());
    }

    private static DataTypeFactory dataTypeFactoryWithRawType(Class<?> rawType) {
        final DataTypeFactoryMock dataTypeFactory = new DataTypeFactoryMock();
        dataTypeFactory.dataType = Optional.of(DataTypeFactoryMock.dummyRaw(rawType));
        return dataTypeFactory;
    }

    private static DataTypeFactory dataTypeFactory() {
        return new DataTypeFactoryMock();
    }
}
