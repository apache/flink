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

package org.apache.flink.table.types.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.test.DataTypeConditions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DataTypeUtils}. */
class DataTypeUtilsTest {

    @Test
    void testAppendRowFields() {
        assertThat(
                        DataTypeUtils.appendRowFields(
                                ROW(
                                        FIELD("a0", BOOLEAN()),
                                        FIELD("a1", DOUBLE()),
                                        FIELD("a2", INT())),
                                Arrays.asList(FIELD("a3", BIGINT()), FIELD("a4", TIMESTAMP(3)))))
                .isEqualTo(
                        ROW(
                                FIELD("a0", BOOLEAN()),
                                FIELD("a1", DOUBLE()),
                                FIELD("a2", INT()),
                                FIELD("a3", BIGINT()),
                                FIELD("a4", TIMESTAMP(3))));

        assertThat(
                        DataTypeUtils.appendRowFields(
                                ROW(), Arrays.asList(FIELD("a", BOOLEAN()), FIELD("b", INT()))))
                .isEqualTo(ROW(FIELD("a", BOOLEAN()), FIELD("b", INT())));
    }

    @Test
    void testIsInternalClass() {
        assertThat(DataTypes.INT()).is(DataTypeConditions.INTERNAL);
        assertThat(DataTypes.INT().notNull().bridgedTo(int.class)).is(DataTypeConditions.INTERNAL);
        assertThat(DataTypes.ROW().bridgedTo(RowData.class)).is(DataTypeConditions.INTERNAL);
        assertThat(DataTypes.ROW()).isNot(DataTypeConditions.INTERNAL);
    }

    @Test
    void testFlattenToDataTypes() {
        assertThat(DataTypeUtils.flattenToDataTypes(INT())).containsOnly(INT());

        assertThat(DataTypeUtils.flattenToDataTypes(ROW(FIELD("a", INT()), FIELD("b", BOOLEAN()))))
                .containsExactly(INT(), BOOLEAN());
    }

    @Test
    void testFlattenToNames() {
        assertThat(DataTypeUtils.flattenToNames(INT(), Collections.emptyList())).containsOnly("f0");

        assertThat(DataTypeUtils.flattenToNames(INT(), Collections.singletonList("f0")))
                .containsOnly("f0_0");

        assertThat(
                        DataTypeUtils.flattenToNames(
                                ROW(FIELD("a", INT()), FIELD("b", BOOLEAN())),
                                Collections.emptyList()))
                .containsExactly("a", "b");
    }

    @Test
    void testExpandRowType() {
        DataType dataType =
                ROW(
                        FIELD("f0", INT()),
                        FIELD("f1", STRING()),
                        FIELD("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
                        FIELD("f3", TIMESTAMP(3)));
        ResolvedSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

        assertThat(schema)
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f0", INT()),
                                Column.physical("f1", STRING()),
                                Column.physical("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
                                Column.physical(
                                        "f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))));
    }

    @Test
    void testExpandLegacyCompositeType() {
        DataType dataType =
                TypeConversions.fromLegacyInfoToDataType(
                        new TupleTypeInfo<>(Types.STRING, Types.INT, Types.SQL_TIMESTAMP));
        ResolvedSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

        assertThat(schema)
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f0", STRING()),
                                Column.physical("f1", INT()),
                                Column.physical("f2", TIMESTAMP(3).bridgedTo(Timestamp.class))));
    }

    @Test
    void testExpandStructuredType() {
        StructuredType logicalType =
                StructuredType.newBuilder(ObjectIdentifier.of("catalog", "database", "type"))
                        .attributes(
                                Arrays.asList(
                                        new StructuredType.StructuredAttribute(
                                                "f0", DataTypes.INT().getLogicalType()),
                                        new StructuredType.StructuredAttribute(
                                                "f1", DataTypes.STRING().getLogicalType()),
                                        new StructuredType.StructuredAttribute(
                                                "f2", DataTypes.TIMESTAMP(5).getLogicalType()),
                                        new StructuredType.StructuredAttribute(
                                                "f3", DataTypes.TIMESTAMP(3).getLogicalType())))
                        .build();

        List<DataType> dataTypes =
                Arrays.asList(
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.TIMESTAMP(5).bridgedTo(Timestamp.class),
                        DataTypes.TIMESTAMP(3));
        FieldsDataType dataType = new FieldsDataType(logicalType, dataTypes);

        ResolvedSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

        assertThat(schema)
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f0", INT()),
                                Column.physical("f1", STRING()),
                                Column.physical("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
                                Column.physical(
                                        "f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))));
    }

    @Test
    void testExpandDistinctType() {
        FieldsDataType dataType =
                (FieldsDataType)
                        ROW(
                                FIELD("f0", INT()),
                                FIELD("f1", STRING()),
                                FIELD("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
                                FIELD("f3", TIMESTAMP(3)));

        LogicalType originalLogicalType = dataType.getLogicalType();
        DistinctType distinctLogicalType =
                DistinctType.newBuilder(
                                ObjectIdentifier.of("catalog", "database", "type"),
                                originalLogicalType)
                        .build();
        DataType distinctDataType = new FieldsDataType(distinctLogicalType, dataType.getChildren());

        ResolvedSchema schema = DataTypeUtils.expandCompositeTypeToSchema(distinctDataType);

        assertThat(schema)
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f0", INT()),
                                Column.physical("f1", STRING()),
                                Column.physical("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
                                Column.physical(
                                        "f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))));
    }

    @Test
    void testExpandThrowExceptionOnAtomicType() {
        assertThatThrownBy(() -> DataTypeUtils.expandCompositeTypeToSchema(DataTypes.TIMESTAMP()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDataTypeValidation() {
        final DataType validDataType = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());

        DataTypeUtils.validateInputDataType(validDataType);
        DataTypeUtils.validateOutputDataType(validDataType);

        final DataType inputOnlyDataType = validDataType.bridgedTo(HashMap.class);
        DataTypeUtils.validateInputDataType(inputOnlyDataType);

        assertThatThrownBy(() -> DataTypeUtils.validateOutputDataType(inputOnlyDataType))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Data type 'MAP<INT, STRING>' does not support an output conversion to class '"
                                + java.util.HashMap.class.getName()
                                + "'.");
    }
}
