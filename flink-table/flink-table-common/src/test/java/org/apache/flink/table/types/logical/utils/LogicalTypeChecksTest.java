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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogicalTypeChecks}. */
class LogicalTypeChecksTest {

    @Test
    void testHasNested() {
        final DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
        assertThat(
                        LogicalTypeChecks.hasNested(
                                dataType.getLogicalType(), t -> t.is(LogicalTypeRoot.VARCHAR)))
                .isTrue();

        assertThat(
                        LogicalTypeChecks.hasNested(
                                dataType.getLogicalType(), t -> t.is(LogicalTypeRoot.ROW)))
                .isTrue();

        assertThat(
                        LogicalTypeChecks.hasNested(
                                dataType.getLogicalType(), t -> t.is(LogicalTypeRoot.BOOLEAN)))
                .isFalse();
    }

    @Test
    void testIsCompositeTypeRowType() {
        DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));

        assertThat(LogicalTypeChecks.isCompositeType(dataType.getLogicalType())).isTrue();
    }

    @Test
    void testIsCompositeTypeDistinctType() {
        DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
        DistinctType distinctType =
                DistinctType.newBuilder(
                                ObjectIdentifier.of("catalog", "database", "type"),
                                dataType.getLogicalType())
                        .build();

        assertThat(LogicalTypeChecks.isCompositeType(distinctType)).isTrue();
    }

    @Test
    void testIsCompositeTypeLegacyCompositeType() {
        DataType dataType =
                TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(Types.STRING, Types.INT));

        assertThat(LogicalTypeChecks.isCompositeType(dataType.getLogicalType())).isTrue();
    }

    @Test
    void testIsCompositeTypeStructuredType() {
        StructuredType logicalType =
                StructuredType.newBuilder(ObjectIdentifier.of("catalog", "database", "type"))
                        .attributes(
                                Arrays.asList(
                                        new StructuredType.StructuredAttribute(
                                                "f0", DataTypes.INT().getLogicalType()),
                                        new StructuredType.StructuredAttribute(
                                                "f1", DataTypes.STRING().getLogicalType())))
                        .build();

        List<DataType> fieldDataTypes = Arrays.asList(DataTypes.INT(), DataTypes.STRING());
        FieldsDataType dataType = new FieldsDataType(logicalType, fieldDataTypes);

        assertThat(LogicalTypeChecks.isCompositeType(dataType.getLogicalType())).isTrue();
    }

    @Test
    void testIsCompositeTypeLegacySimpleType() {
        DataType dataType = TypeConversions.fromLegacyInfoToDataType(Types.STRING);

        assertThat(LogicalTypeChecks.isCompositeType(dataType.getLogicalType())).isFalse();
    }

    @Test
    void testIsCompositeTypeSimpleType() {
        DataType dataType = DataTypes.TIMESTAMP();

        assertThat(LogicalTypeChecks.isCompositeType(dataType.getLogicalType())).isFalse();
    }

    @Test
    void testFieldNameExtraction() {
        DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
        assertThat(LogicalTypeChecks.getFieldNames(dataType.getLogicalType()))
                .containsExactly("f0", "f1");
    }

    @Test
    void testFieldCountExtraction() {
        DataType dataType = ROW(FIELD("f0", INT()), FIELD("f1", STRING()));
        assertThat(LogicalTypeChecks.getFieldCount(dataType.getLogicalType())).isEqualTo(2);
    }
}
