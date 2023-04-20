/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for TableSchemaUtils. */
class TableSchemaUtilsTest {

    @Test
    void testBuilderWithGivenSchema() {
        TableSchema oriSchema =
                TableSchema.builder()
                        .field("a", DataTypes.INT().notNull())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.INT(), "a + 1")
                        .field("t", DataTypes.TIMESTAMP(3))
                        .primaryKey("ct1", new String[] {"a"})
                        .watermark("t", "t", DataTypes.TIMESTAMP(3))
                        .build();
        TableSchema newSchema = TableSchemaUtils.builderWithGivenSchema(oriSchema).build();
        assertThat(newSchema).isEqualTo(oriSchema);
    }

    @Test
    void testDropConstraint() {
        TableSchema originalSchema =
                TableSchema.builder()
                        .field("a", DataTypes.INT().notNull())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.INT(), "a + 1")
                        .field("t", DataTypes.TIMESTAMP(3))
                        .primaryKey("ct1", new String[] {"a"})
                        .watermark("t", "t", DataTypes.TIMESTAMP(3))
                        .build();
        TableSchema newSchema = TableSchemaUtils.dropConstraint(originalSchema, "ct1");
        TableSchema expectedSchema =
                TableSchema.builder()
                        .field("a", DataTypes.INT().notNull())
                        .field("b", DataTypes.STRING())
                        .field("c", DataTypes.INT(), "a + 1")
                        .field("t", DataTypes.TIMESTAMP(3))
                        .watermark("t", "t", DataTypes.TIMESTAMP(3))
                        .build();
        assertThat(newSchema).isEqualTo(expectedSchema);

        // Drop non-exist constraint.
        assertThatThrownBy(() -> TableSchemaUtils.dropConstraint(originalSchema, "ct2"))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Constraint ct2 to drop does not exist");
    }

    @Test
    void testRemoveTimeAttribute() {
        DataType rowTimeType =
                DataTypeUtils.replaceLogicalType(
                        DataTypes.TIMESTAMP(3), new TimestampType(true, TimestampKind.ROWTIME, 3));
        ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("id", DataTypes.INT().notNull()),
                                Column.physical("t", rowTimeType),
                                Column.computed(
                                        "date",
                                        ResolvedExpressionMock.of(DataTypes.DATE(), "TO_DATE(t)")),
                                Column.metadata("metadata-1", DataTypes.INT(), "metadata", false)),
                        Collections.singletonList(
                                WatermarkSpec.of("t", ResolvedExpressionMock.of(rowTimeType, "t"))),
                        UniqueConstraint.primaryKey("test-pk", Collections.singletonList("id")));
        assertThat(TableSchemaUtils.removeTimeAttributeFromResolvedSchema(schema))
                .isEqualTo(
                        new ResolvedSchema(
                                Arrays.asList(
                                        Column.physical("id", DataTypes.INT().notNull()),
                                        Column.physical("t", DataTypes.TIMESTAMP(3)),
                                        Column.computed(
                                                "date",
                                                new ResolvedExpressionMock(
                                                        DataTypes.DATE(), () -> "TO_DATE(t)")),
                                        Column.metadata(
                                                "metadata-1", DataTypes.INT(), "metadata", false)),
                                Collections.singletonList(
                                        WatermarkSpec.of(
                                                "t", ResolvedExpressionMock.of(rowTimeType, "t"))),
                                UniqueConstraint.primaryKey(
                                        "test-pk", Collections.singletonList("id"))));
    }
}
