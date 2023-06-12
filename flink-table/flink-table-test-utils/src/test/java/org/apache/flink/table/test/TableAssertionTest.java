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

package org.apache.flink.table.test;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.apache.flink.table.test.TableAssertions.assertThatRows;

/** Tests for {@link TableAssertions} assertions. */
class TableAssertionTest {

    @Test
    void testAssertRowDataWithConversion() {
        DataType dataType =
                ROW(
                        FIELD("a", INT()),
                        FIELD("b", STRING()),
                        FIELD("c", ARRAY(BOOLEAN().notNull())));

        GenericRowData genericRowData =
                GenericRowData.of(
                        10,
                        StringData.fromString("my string"),
                        new GenericArrayData(new boolean[] {true, false}));
        BinaryRowData binaryRowData =
                new RowDataSerializer((RowType) dataType.getLogicalType())
                        .toBinaryRow(genericRowData);
        Row row = Row.of(10, "my string", new boolean[] {true, false});

        // Test equality with RowData
        assertThat(binaryRowData)
                .asGeneric(dataType)
                .isEqualTo(genericRowData)
                .isEqualTo(binaryRowData.copy());
        assertThatRows(binaryRowData)
                .asGeneric(dataType)
                .containsOnly(genericRowData)
                .containsOnly(binaryRowData);

        // Test equality with Row
        assertThat(binaryRowData).asRow(dataType).isEqualTo(row);
        assertThatRows(binaryRowData).asRows(dataType).containsOnly(row);
    }
}
