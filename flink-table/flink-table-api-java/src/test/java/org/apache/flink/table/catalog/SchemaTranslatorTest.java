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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.SchemaTranslator.ConsumingResult;
import org.apache.flink.table.catalog.SchemaTranslator.ProducingResult;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SchemaTranslator}. */
class SchemaTranslatorTest {

    @Test
    void testInputFromRow() {
        final TypeInformation<?> inputTypeInfo =
                Types.ROW(Types.ROW(Types.INT, Types.BOOLEAN), Types.ENUM(DayOfWeek.class));

        final ConsumingResult result =
                SchemaTranslator.createConsumingResult(
                        dataTypeFactoryWithRawType(DayOfWeek.class), inputTypeInfo, null);

        assertThat(result.getPhysicalDataType())
                .isEqualTo(
                        ROW(
                                        FIELD(
                                                "f0",
                                                ROW(FIELD("f0", INT()), FIELD("f1", BOOLEAN()))),
                                        FIELD("f1", DataTypeFactoryMock.dummyRaw(DayOfWeek.class)))
                                .notNull());

        assertThat(result.isTopLevelRecord()).isTrue();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("f0", ROW(FIELD("f0", INT()), FIELD("f1", BOOLEAN())))
                                .column("f1", DataTypeFactoryMock.dummyRaw(DayOfWeek.class))
                                .build());

        assertThat(result.getProjections()).isNull();
    }

    @Test
    void testOutputToRowDataType() {
        final ResolvedSchema inputSchema =
                ResolvedSchema.of(
                        Column.physical("c", INT()),
                        Column.physical("a", BOOLEAN()),
                        Column.physical("B", DOUBLE())); // case-insensitive mapping

        final DataType physicalDataType =
                ROW(FIELD("a", BOOLEAN()), FIELD("b", DOUBLE()), FIELD("c", INT()));

        final ProducingResult result =
                SchemaTranslator.createProducingResult(
                        dataTypeFactory(), inputSchema, physicalDataType);

        assertThat(result.getProjections()).hasValue(Arrays.asList("a", "B", "c"));

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("a", BOOLEAN())
                                .column("b", DOUBLE())
                                .column("c", INT())
                                .build());

        assertThat(result.getPhysicalDataType()).hasValue(physicalDataType);
    }

    @Test
    void testInputFromAtomic() {
        final TypeInformation<?> inputTypeInfo = Types.GENERIC(Row.class);

        final ConsumingResult result =
                SchemaTranslator.createConsumingResult(
                        dataTypeFactoryWithRawType(Row.class), inputTypeInfo, null);

        assertThat(result.getPhysicalDataType()).isEqualTo(DataTypeFactoryMock.dummyRaw(Row.class));

        assertThat(result.isTopLevelRecord()).isFalse();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("f0", DataTypeFactoryMock.dummyRaw(Row.class))
                                .build());

        assertThat(result.getProjections()).isNull();
    }

    @Test
    void testOutputToAtomicDataType() {
        final ResolvedSchema inputSchema = ResolvedSchema.of(Column.physical("a", INT()));

        final ProducingResult result =
                SchemaTranslator.createProducingResult(dataTypeFactory(), inputSchema, INT());

        assertThat(result.getProjections()).isEmpty();

        assertThat(result.getSchema()).isEqualTo(Schema.newBuilder().column("f0", INT()).build());

        assertThat(result.getPhysicalDataType()).hasValue(INT());
    }

    @Test
    void testInputFromRowWithNonPhysicalDeclaredSchema() {
        final TypeInformation<?> inputTypeInfo = Types.ROW(Types.INT, Types.LONG);

        final ConsumingResult result =
                SchemaTranslator.createConsumingResult(
                        dataTypeFactory(),
                        inputTypeInfo,
                        Schema.newBuilder()
                                .columnByExpression("computed", "f1 + 42")
                                .columnByExpression("computed2", "f1 - 1")
                                .primaryKeyNamed("pk", "f0")
                                .build());

        assertThat(result.getPhysicalDataType())
                .isEqualTo(ROW(FIELD("f0", INT()), FIELD("f1", BIGINT())).notNull());

        assertThat(result.isTopLevelRecord()).isTrue();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("f0", INT().notNull()) // not null due to primary key
                                .column("f1", BIGINT())
                                .columnByExpression("computed", "f1 + 42")
                                .columnByExpression("computed2", "f1 - 1")
                                .primaryKeyNamed("pk", "f0")
                                .build());

        assertThat(result.getProjections()).isNull();
    }

    @Test
    void testInputFromRowWithPhysicalDeclaredSchema() {
        final TypeInformation<?> inputTypeInfo =
                Types.ROW(Types.INT, Types.LONG, Types.GENERIC(BigDecimal.class), Types.BOOLEAN);

        final ConsumingResult result =
                SchemaTranslator.createConsumingResult(
                        dataTypeFactoryWithRawType(BigDecimal.class),
                        inputTypeInfo,
                        Schema.newBuilder()
                                .primaryKeyNamed("pk", "f0")
                                .column("f1", BIGINT()) // reordered
                                .column("f0", INT())
                                .columnByExpression("computed", "f1 + 42")
                                .column("f2", DECIMAL(10, 2)) // enriches
                                .columnByExpression("computed2", "f1 - 1")
                                .build());

        assertThat(result.getPhysicalDataType())
                .isEqualTo(
                        ROW(
                                        FIELD("f0", INT()),
                                        FIELD("f1", BIGINT()),
                                        FIELD("f2", DECIMAL(10, 2)),
                                        FIELD("f3", BOOLEAN()))
                                .notNull());

        assertThat(result.isTopLevelRecord()).isTrue();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("f0", INT())
                                .column("f1", BIGINT())
                                .column("f2", DECIMAL(10, 2))
                                .column("f3", BOOLEAN())
                                .columnByExpression("computed", "f1 + 42")
                                .columnByExpression("computed2", "f1 - 1")
                                .primaryKeyNamed("pk", "f0")
                                .build());

        assertThat(result.getProjections())
                .isEqualTo(Arrays.asList("f1", "f0", "computed", "f2", "computed2"));
    }

    @Test
    void testInputFromAtomicWithPhysicalDeclaredSchema() {
        final TypeInformation<?> inputTypeInfo = Types.GENERIC(Row.class);

        final ConsumingResult result =
                SchemaTranslator.createConsumingResult(
                        dataTypeFactoryWithRawType(Row.class),
                        inputTypeInfo,
                        Schema.newBuilder()
                                .columnByExpression("f0_0", "f0.f0_0")
                                .column("f0", ROW(FIELD("f0_0", INT()), FIELD("f0_1", BOOLEAN())))
                                .columnByExpression("f0_1", "f0.f0_1")
                                .build());

        assertThat(result.getPhysicalDataType())
                .isEqualTo(ROW(FIELD("f0_0", INT()), FIELD("f0_1", BOOLEAN())));

        assertThat(result.isTopLevelRecord()).isFalse();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("f0", ROW(FIELD("f0_0", INT()), FIELD("f0_1", BOOLEAN())))
                                .columnByExpression("f0_0", "f0.f0_0")
                                .columnByExpression("f0_1", "f0.f0_1")
                                .build());

        assertThat(result.getProjections()).isEqualTo(Arrays.asList("f0_0", "f0", "f0_1"));
    }

    @Test
    void testInvalidDeclaredSchemaColumn() {
        final TypeInformation<?> inputTypeInfo = Types.ROW(Types.INT, Types.LONG);

        assertThatThrownBy(
                        () ->
                                SchemaTranslator.createConsumingResult(
                                        dataTypeFactory(),
                                        inputTypeInfo,
                                        Schema.newBuilder().column("INVALID", BIGINT()).build()))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Unable to find a field named 'INVALID' in the physical data type"));
    }

    @Test
    void testOutputToNoSchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", BIGINT()),
                        Column.metadata("rowtime", TIMESTAMP_LTZ(3), null, false),
                        Column.physical("name", STRING()));

        final ProducingResult result = SchemaTranslator.createProducingResult(tableSchema, null);

        assertThat(result.getProjections()).isEmpty();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", BIGINT())
                                .column("rowtime", TIMESTAMP_LTZ(3)) // becomes physical
                                .column("name", STRING())
                                .build());

        assertThat(result.getPhysicalDataType()).isEmpty();
    }

    @Test
    void testOutputToEmptySchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", BIGINT()),
                        Column.metadata("rowtime", TIMESTAMP_LTZ(3), null, false),
                        Column.physical("name", STRING()));

        final ProducingResult result =
                SchemaTranslator.createProducingResult(tableSchema, Schema.derived());

        assertThat(result.getProjections()).isEmpty();

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", BIGINT())
                                .column("rowtime", TIMESTAMP_LTZ(3)) // becomes physical
                                .column("name", STRING())
                                .build());

        assertThat(result.getPhysicalDataType()).isEmpty();
    }

    @Test
    void testOutputToPartialSchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", BIGINT().notNull()),
                        Column.physical("name", STRING()),
                        Column.metadata("rowtime", TIMESTAMP_LTZ(3), null, false));

        final ProducingResult result =
                SchemaTranslator.createProducingResult(
                        tableSchema,
                        Schema.newBuilder()
                                .columnByExpression("computed", "f1 + 42")
                                .columnByMetadata("rowtime", TIMESTAMP_LTZ(3))
                                .primaryKey("id")
                                .build());

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", BIGINT().notNull())
                                .column("name", STRING())
                                .columnByExpression("computed", "f1 + 42")
                                .columnByMetadata("rowtime", TIMESTAMP_LTZ(3)) // becomes metadata
                                .primaryKey("id")
                                .build());
    }

    @Test
    void testOutputToDeclaredSchema() {
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(
                        Column.physical("id", BIGINT()),
                        Column.physical("rowtime", TIMESTAMP_LTZ(3)),
                        Column.physical("name", STRING()));

        final ProducingResult result =
                SchemaTranslator.createProducingResult(
                        tableSchema,
                        Schema.newBuilder()
                                .column("id", BIGINT())
                                .columnByMetadata("rowtime", TIMESTAMP_LTZ(3))
                                .column("name", STRING().bridgedTo(StringData.class))
                                .build());

        assertThat(result.getSchema())
                .isEqualTo(
                        Schema.newBuilder()
                                .column("id", BIGINT())
                                .columnByMetadata("rowtime", TIMESTAMP_LTZ(3))
                                .column("name", STRING().bridgedTo(StringData.class))
                                .build());
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
