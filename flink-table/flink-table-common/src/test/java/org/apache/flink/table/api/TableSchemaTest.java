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

package org.apache.flink.table.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableSchema}. */
class TableSchemaTest {

    private static final String WATERMARK_EXPRESSION = "localtimestamp";
    private static final String WATERMARK_EXPRESSION_TS_LTZ = "now()";
    private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
    private static final DataType WATERMARK_TS_LTZ_DATATYPE = DataTypes.TIMESTAMP_LTZ(3);

    @Test
    void testTableSchema() {
        TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.physical("f0", DataTypes.BIGINT()))
                        .add(
                                TableColumn.physical(
                                        "f1",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("q1", DataTypes.STRING()),
                                                DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3)))))
                        .add(TableColumn.physical("f2", DataTypes.STRING()))
                        .add(TableColumn.computed("f3", DataTypes.BIGINT(), "f0 + 1"))
                        .add(TableColumn.metadata("f4", DataTypes.BIGINT(), "other.key", true))
                        .watermark("f1.q2", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
                        .build();

        // test toString()
        String expected =
                "root\n"
                        + " |-- f0: BIGINT\n"
                        + " |-- f1: ROW<`q1` STRING, `q2` TIMESTAMP(3)>\n"
                        + " |-- f2: STRING\n"
                        + " |-- f3: BIGINT AS f0 + 1\n"
                        + " |-- f4: BIGINT METADATA FROM 'other.key' VIRTUAL\n"
                        + " |-- WATERMARK FOR f1.q2: TIMESTAMP(3) AS localtimestamp\n";
        assertThat(schema.toString()).isEqualTo(expected);

        // test getFieldNames and getFieldDataType
        assertThat(schema.getFieldName(2)).isEqualTo(Optional.of("f2"));
        assertThat(schema.getFieldDataType(3)).isEqualTo(Optional.of(DataTypes.BIGINT()));
        assertThat(schema.getTableColumn(3))
                .isEqualTo(Optional.of(TableColumn.computed("f3", DataTypes.BIGINT(), "f0 + 1")));
        assertThat(schema.getFieldDataType("f2")).isEqualTo(Optional.of(DataTypes.STRING()));
        assertThat(schema.getFieldDataType("f1").map(r -> r.getChildren().get(0)))
                .isEqualTo(Optional.of(DataTypes.STRING()));
        assertThat(schema.getFieldName(5)).isNotPresent();
        assertThat(schema.getFieldType(-1)).isNotPresent();
        assertThat(schema.getFieldType("c")).isNotPresent();
        assertThat(schema.getFieldDataType("f1.q1")).isNotPresent();
        assertThat(schema.getFieldDataType("f1.q3")).isNotPresent();

        // test copy() and equals()
        assertThat(schema.copy()).isEqualTo(schema);
        assertThat(schema.copy().hashCode()).isEqualTo(schema.hashCode());
    }

    @Test
    void testWatermarkOnTimestampLtz() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .field("f0", DataTypes.TIMESTAMP())
                        .field(
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("q1", DataTypes.STRING()),
                                        DataTypes.FIELD("q2", DataTypes.TIMESTAMP_LTZ(3))))
                        .watermark("f1.q2", WATERMARK_EXPRESSION_TS_LTZ, WATERMARK_TS_LTZ_DATATYPE)
                        .build();

        // test toString()
        String expected =
                "root\n"
                        + " |-- f0: TIMESTAMP(6)\n"
                        + " |-- f1: ROW<`q1` STRING, `q2` TIMESTAMP_LTZ(3)>\n"
                        + " |-- WATERMARK FOR f1.q2: TIMESTAMP_LTZ(3) AS now()\n";
        assertThat(tableSchema.toString()).isEqualTo(expected);
    }

    @Test
    void testPersistedRowDataType() {
        final TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.physical("f0", DataTypes.BIGINT()))
                        .add(TableColumn.metadata("f1", DataTypes.BIGINT(), true))
                        .add(TableColumn.metadata("f2", DataTypes.BIGINT(), false))
                        .add(TableColumn.physical("f3", DataTypes.STRING()))
                        .add(TableColumn.computed("f4", DataTypes.BIGINT(), "f0 + 1"))
                        .add(TableColumn.metadata("f5", DataTypes.BIGINT(), false))
                        .build();

        final DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.BIGINT()),
                                DataTypes.FIELD("f2", DataTypes.BIGINT()),
                                DataTypes.FIELD("f3", DataTypes.STRING()),
                                DataTypes.FIELD("f5", DataTypes.BIGINT()))
                        .notNull();

        assertThat(schema.toPersistedRowDataType()).isEqualTo(expectedDataType);
    }

    @Test
    void testPhysicalRowDataType() {
        final TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.physical("f0", DataTypes.BIGINT()))
                        .add(TableColumn.metadata("f1", DataTypes.BIGINT(), true))
                        .add(TableColumn.metadata("f2", DataTypes.BIGINT(), false))
                        .add(TableColumn.physical("f3", DataTypes.STRING()))
                        .add(TableColumn.computed("f4", DataTypes.BIGINT(), "f0 + 1"))
                        .add(TableColumn.metadata("f5", DataTypes.BIGINT(), false))
                        .build();

        final DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.BIGINT()),
                                DataTypes.FIELD("f3", DataTypes.STRING()))
                        .notNull();

        assertThat(schema.toPhysicalRowDataType()).isEqualTo(expectedDataType);
    }

    @Test
    void testRowDataType() {
        final TableSchema schema =
                TableSchema.builder()
                        .add(TableColumn.physical("f0", DataTypes.BIGINT()))
                        .add(TableColumn.metadata("f1", DataTypes.BIGINT(), true))
                        .add(TableColumn.metadata("f2", DataTypes.BIGINT(), false))
                        .add(TableColumn.physical("f3", DataTypes.STRING()))
                        .add(TableColumn.computed("f4", DataTypes.BIGINT(), "f0 + 1"))
                        .add(TableColumn.metadata("f5", DataTypes.BIGINT(), false))
                        .build();

        final DataType expectedDataType =
                DataTypes.ROW(
                                DataTypes.FIELD("f0", DataTypes.BIGINT()),
                                DataTypes.FIELD("f1", DataTypes.BIGINT()),
                                DataTypes.FIELD("f2", DataTypes.BIGINT()),
                                DataTypes.FIELD("f3", DataTypes.STRING()),
                                DataTypes.FIELD("f4", DataTypes.BIGINT()),
                                DataTypes.FIELD("f5", DataTypes.BIGINT()))
                        .notNull();

        assertThat(schema.toRowDataType()).isEqualTo(expectedDataType);
    }

    @Test
    void testWatermarkOnDifferentFields() {
        // column_name, column_type, exception_msg
        List<Tuple3<String, DataType, String>> testData = new ArrayList<>();
        testData.add(Tuple3.of("a", DataTypes.BIGINT(), "but is of type 'BIGINT'"));
        testData.add(Tuple3.of("b", DataTypes.STRING(), "but is of type 'STRING'"));
        testData.add(Tuple3.of("c", DataTypes.INT(), "but is of type 'INT'"));
        testData.add(Tuple3.of("d", DataTypes.TIMESTAMP(), "PASS"));
        testData.add(Tuple3.of("e", DataTypes.TIMESTAMP(0), "PASS"));
        testData.add(Tuple3.of("f", DataTypes.TIMESTAMP(3), "PASS"));
        testData.add(Tuple3.of("g", DataTypes.TIMESTAMP(9), "PASS"));
        testData.add(
                Tuple3.of(
                        "h",
                        DataTypes.TIMESTAMP_WITH_TIME_ZONE(3),
                        "but is of type 'TIMESTAMP(3) WITH TIME ZONE'"));

        testData.forEach(
                t -> {
                    TableSchema.Builder builder = TableSchema.builder();
                    testData.forEach(e -> builder.field(e.f0, e.f1));
                    builder.watermark(t.f0, WATERMARK_EXPRESSION, WATERMARK_DATATYPE);
                    if (t.f2.equals("PASS")) {
                        TableSchema schema = builder.build();
                        assertThat(schema.getWatermarkSpecs()).hasSize(1);
                        assertThat(schema.getWatermarkSpecs().get(0).getRowtimeAttribute())
                                .isEqualTo(t.f0);
                    } else {
                        assertThatThrownBy(builder::build)
                                .isInstanceOf(ValidationException.class)
                                .hasMessageContaining(t.f2);
                    }
                });
    }

    @Test
    void testWatermarkOnNestedField() {
        TableSchema schema =
                TableSchema.builder()
                        .field("f0", DataTypes.BIGINT())
                        .field(
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("q1", DataTypes.STRING()),
                                        DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3)),
                                        DataTypes.FIELD(
                                                "q3",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "t1", DataTypes.TIMESTAMP(3)),
                                                        DataTypes.FIELD(
                                                                "t2", DataTypes.STRING())))))
                        .watermark("f1.q3.t1", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
                        .build();

        assertThat(schema.getWatermarkSpecs()).hasSize(1);
        assertThat(schema.getWatermarkSpecs().get(0).getRowtimeAttribute()).isEqualTo("f1.q3.t1");
    }

    @Test
    void testWatermarkOnNonExistedField() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT())
                                        .field(
                                                "f1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("q1", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "q2", DataTypes.TIMESTAMP(3))))
                                        .watermark(
                                                "f1.q0", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage("Rowtime attribute 'f1.q0' is not defined in schema.");
    }

    @Test
    void testMultipleWatermarks() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.TIMESTAMP())
                                        .field(
                                                "f1",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD("q1", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "q2", DataTypes.TIMESTAMP(3))))
                                        .watermark(
                                                "f1.q2", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
                                        .watermark("f0", WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Multiple watermark definition is not supported yet.");
    }

    @Test
    void testDifferentWatermarkStrategyOutputTypes() {
        List<Tuple2<DataType, String>> testData = new ArrayList<>();
        testData.add(Tuple2.of(DataTypes.BIGINT(), "but is of type 'BIGINT'"));
        testData.add(Tuple2.of(DataTypes.STRING(), "but is of type 'STRING'"));
        testData.add(Tuple2.of(DataTypes.INT(), "but is of type 'INT'"));
        testData.add(Tuple2.of(DataTypes.TIMESTAMP(), "PASS"));
        testData.add(Tuple2.of(DataTypes.TIMESTAMP(0), "PASS"));
        testData.add(Tuple2.of(DataTypes.TIMESTAMP(3), "PASS"));
        testData.add(Tuple2.of(DataTypes.TIMESTAMP(9), "PASS"));
        testData.add(
                Tuple2.of(
                        DataTypes.TIMESTAMP_WITH_TIME_ZONE(3),
                        "but is of type 'TIMESTAMP(3) WITH TIME ZONE'"));

        testData.forEach(
                t -> {
                    TableSchema.Builder builder =
                            TableSchema.builder()
                                    .field("f0", DataTypes.TIMESTAMP())
                                    .watermark("f0", "f0 - INTERVAL '5' SECOND", t.f0);
                    if (t.f1.equals("PASS")) {
                        TableSchema schema = builder.build();
                        assertThat(schema.getWatermarkSpecs()).hasSize(1);
                    } else {
                        assertThatThrownBy(builder::build)
                                .isInstanceOf(ValidationException.class)
                                .hasMessageContaining(t.f1);
                    }
                });
    }

    /*
    CONSTRAINTS TESTS
    */
    @Test
    void testPrimaryKeyPrinting() {
        TableSchema schema =
                TableSchema.builder()
                        .field("f0", DataTypes.BIGINT().notNull())
                        .field("f1", DataTypes.STRING().notNull())
                        .field("f2", DataTypes.DOUBLE().notNull())
                        .primaryKey("pk", new String[] {"f0", "f2"})
                        .build();

        assertThat(schema.toString())
                .isEqualTo(
                        "root\n"
                                + " |-- f0: BIGINT NOT NULL\n"
                                + " |-- f1: STRING NOT NULL\n"
                                + " |-- f2: DOUBLE NOT NULL\n"
                                + " |-- CONSTRAINT pk PRIMARY KEY (f0, f2)\n");
    }

    @Test
    void testPrimaryKeyColumnsIndices() {
        TableSchema schema =
                TableSchema.builder()
                        .field("f0", DataTypes.BIGINT().notNull())
                        .field("f1", DataTypes.STRING().notNull())
                        .field("f2", DataTypes.DOUBLE().notNull())
                        .primaryKey("pk", new String[] {"f0", "f2"})
                        .build();

        UniqueConstraint expectedKey = UniqueConstraint.primaryKey("pk", Arrays.asList("f0", "f2"));

        assertThat(schema.getPrimaryKey().get()).isEqualTo(expectedKey);
    }

    @Test
    void testPrimaryKeyLazilyDefinedColumns() {
        TableSchema schema =
                TableSchema.builder()
                        .field("f0", DataTypes.BIGINT().notNull())
                        .primaryKey("pk", new String[] {"f0", "f2"})
                        .field("f1", DataTypes.STRING().notNull())
                        .field("f2", DataTypes.DOUBLE().notNull())
                        .build();

        UniqueConstraint expectedKey = UniqueConstraint.primaryKey("pk", Arrays.asList("f0", "f2"));

        assertThat(schema.getPrimaryKey().get()).isEqualTo(expectedKey);
    }

    @Test
    void testPrimaryKeyNoColumn() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT().notNull())
                                        .primaryKey("pk", new String[] {"f0", "f2"})
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage("Could not create a PRIMARY KEY 'pk'. Column 'f2' does not exist.");
    }

    @Test
    void testPrimaryKeyNullableColumn() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT())
                                        .primaryKey("pk", new String[] {"f0"})
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage("Could not create a PRIMARY KEY 'pk'. Column 'f0' is nullable.");
    }

    @Test
    void testPrimaryKeyGeneratedColumn() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT().notNull(), "123")
                                        .primaryKey("pk", new String[] {"f0", "f2"})
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Could not create a PRIMARY KEY 'pk'. Column 'f0' is not a physical column.");
    }

    @Test
    void testPrimaryKeyNameMustNotBeNull() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT())
                                        .primaryKey(null, new String[] {"f0", "f2"})
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage("PRIMARY KEY's name can not be null or empty.");
    }

    @Test
    void testPrimaryKeyNameMustNotBeEmpty() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT())
                                        .primaryKey("", new String[] {"f0", "f2"})
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage("PRIMARY KEY's name can not be null or empty.");
    }

    @Test
    void testPrimaryKeyNoColumns() {
        assertThatThrownBy(
                        () ->
                                TableSchema.builder()
                                        .field("f0", DataTypes.BIGINT())
                                        .primaryKey("pk", new String[] {})
                                        .build())
                .isInstanceOf(ValidationException.class)
                .hasMessage("PRIMARY KEY constraint must be defined for at least a single column.");
    }
}
