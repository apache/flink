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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;

import org.junit.Test;

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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link DataTypeUtils}. */
public class DataTypeUtilsTest {

    @Test
    public void testProjectRow() {
        final DataType thirdLevelRow =
                ROW(FIELD("c0", BOOLEAN()), FIELD("c1", DOUBLE()), FIELD("c2", INT()));
        final DataType secondLevelRow =
                ROW(FIELD("b0", BOOLEAN()), FIELD("b1", thirdLevelRow), FIELD("b2", INT()));
        final DataType topLevelRow =
                ROW(FIELD("a0", INT()), FIELD("a1", secondLevelRow), FIELD("a1_b1_c0", INT()));

        assertThat(
                DataTypeUtils.projectRow(topLevelRow, new int[][] {{0}, {1, 1, 0}}),
                equalTo(ROW(FIELD("a0", INT()), FIELD("a1_b1_c0", BOOLEAN()))));

        assertThat(
                DataTypeUtils.projectRow(topLevelRow, new int[][] {{1, 1}, {0}}),
                equalTo(ROW(FIELD("a1_b1", thirdLevelRow), FIELD("a0", INT()))));

        assertThat(
                DataTypeUtils.projectRow(
                        topLevelRow, new int[][] {{1, 1, 2}, {1, 1, 1}, {1, 1, 0}}),
                equalTo(
                        ROW(
                                FIELD("a1_b1_c2", INT()),
                                FIELD("a1_b1_c1", DOUBLE()),
                                FIELD("a1_b1_c0", BOOLEAN()))));

        assertThat(
                DataTypeUtils.projectRow(topLevelRow, new int[][] {{1, 1, 0}, {2}}),
                equalTo(ROW(FIELD("a1_b1_c0", BOOLEAN()), FIELD("a1_b1_c0_$0", INT()))));
    }

    @Test
    public void testAppendRowFields() {
        {
            final DataType row =
                    ROW(FIELD("a0", BOOLEAN()), FIELD("a1", DOUBLE()), FIELD("a2", INT()));

            final DataType expectedRow =
                    ROW(
                            FIELD("a0", BOOLEAN()),
                            FIELD("a1", DOUBLE()),
                            FIELD("a2", INT()),
                            FIELD("a3", BIGINT()),
                            FIELD("a4", TIMESTAMP(3)));

            assertThat(
                    DataTypeUtils.appendRowFields(
                            row, Arrays.asList(FIELD("a3", BIGINT()), FIELD("a4", TIMESTAMP(3)))),
                    equalTo(expectedRow));
        }

        {
            final DataType row = ROW();

            final DataType expectedRow = ROW(FIELD("a", BOOLEAN()), FIELD("b", INT()));

            assertThat(
                    DataTypeUtils.appendRowFields(
                            row, Arrays.asList(FIELD("a", BOOLEAN()), FIELD("b", INT()))),
                    equalTo(expectedRow));
        }
    }

    @Test
    public void testIsInternalClass() {
        assertTrue(DataTypeUtils.isInternal(DataTypes.INT()));
        assertTrue(DataTypeUtils.isInternal(DataTypes.INT().notNull().bridgedTo(int.class)));
        assertTrue(DataTypeUtils.isInternal(DataTypes.ROW().bridgedTo(RowData.class)));
        assertFalse(DataTypeUtils.isInternal(DataTypes.ROW()));
    }

    @Test
    public void testFlattenToDataTypes() {
        assertThat(
                DataTypeUtils.flattenToDataTypes(INT()), equalTo(Collections.singletonList(INT())));

        assertThat(
                DataTypeUtils.flattenToDataTypes(ROW(FIELD("a", INT()), FIELD("b", BOOLEAN()))),
                equalTo(Arrays.asList(INT(), BOOLEAN())));
    }

    @Test
    public void testFlattenToNames() {
        assertThat(
                DataTypeUtils.flattenToNames(INT(), Collections.emptyList()),
                equalTo(Collections.singletonList("f0")));

        assertThat(
                DataTypeUtils.flattenToNames(INT(), Collections.singletonList("f0")),
                equalTo(Collections.singletonList("f0_0")));

        assertThat(
                DataTypeUtils.flattenToNames(
                        ROW(FIELD("a", INT()), FIELD("b", BOOLEAN())), Collections.emptyList()),
                equalTo(Arrays.asList("a", "b")));
    }

    @Test
    public void testExpandRowType() {
        DataType dataType =
                ROW(
                        FIELD("f0", INT()),
                        FIELD("f1", STRING()),
                        FIELD("f2", TIMESTAMP(5).bridgedTo(Timestamp.class)),
                        FIELD("f3", TIMESTAMP(3)));
        TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

        assertThat(
                schema,
                equalTo(
                        TableSchema.builder()
                                .field("f0", INT())
                                .field("f1", STRING())
                                .field("f2", TIMESTAMP(5).bridgedTo(Timestamp.class))
                                .field("f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))
                                .build()));
    }

    @Test
    public void testExpandLegacyCompositeType() {
        DataType dataType =
                TypeConversions.fromLegacyInfoToDataType(
                        new TupleTypeInfo<>(Types.STRING, Types.INT, Types.SQL_TIMESTAMP));
        TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

        assertThat(
                schema,
                equalTo(
                        TableSchema.builder()
                                .field("f0", STRING())
                                .field("f1", INT())
                                .field("f2", TIMESTAMP(3).bridgedTo(Timestamp.class))
                                .build()));
    }

    @Test
    public void testExpandStructuredType() {
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

        TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(dataType);

        assertThat(
                schema,
                equalTo(
                        TableSchema.builder()
                                .field("f0", INT())
                                .field("f1", STRING())
                                .field("f2", TIMESTAMP(5).bridgedTo(Timestamp.class))
                                .field("f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))
                                .build()));
    }

    @Test
    public void testExpandDistinctType() {
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

        TableSchema schema = DataTypeUtils.expandCompositeTypeToSchema(distinctDataType);

        assertThat(
                schema,
                equalTo(
                        TableSchema.builder()
                                .field("f0", INT())
                                .field("f1", STRING())
                                .field("f2", TIMESTAMP(5).bridgedTo(Timestamp.class))
                                .field("f3", TIMESTAMP(3).bridgedTo(LocalDateTime.class))
                                .build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExpandThrowExceptionOnAtomicType() {
        DataTypeUtils.expandCompositeTypeToSchema(DataTypes.TIMESTAMP());
    }

    @Test
    public void testDataTypeValidation() {
        final DataType validDataType = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());

        DataTypeUtils.validateInputDataType(validDataType);
        DataTypeUtils.validateOutputDataType(validDataType);

        final DataType inputOnlyDataType = validDataType.bridgedTo(HashMap.class);
        DataTypeUtils.validateInputDataType(inputOnlyDataType);
        try {
            DataTypeUtils.validateOutputDataType(inputOnlyDataType);
            fail();
        } catch (ValidationException e) {
            assertEquals(
                    e.getMessage(),
                    "Data type 'MAP<INT, STRING>' does not support an output conversion to class '"
                            + java.util.HashMap.class.getName()
                            + "'.");
        }
    }
}
