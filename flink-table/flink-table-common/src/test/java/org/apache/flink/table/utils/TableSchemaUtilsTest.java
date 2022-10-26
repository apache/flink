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

import org.junit.jupiter.api.Test;

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
}
