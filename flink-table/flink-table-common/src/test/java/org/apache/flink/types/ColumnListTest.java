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

package org.apache.flink.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ColumnList}. */
class ColumnListTest {

    @Test
    void testWithNamesAndDataTypes() {
        final List<String> names = List.of("a", "b", "c");
        final List<DataType> dataTypes =
                List.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN());

        final ColumnList columnList = ColumnList.of(names, dataTypes);

        assertThat(columnList.getNames()).isEqualTo(names);
        assertThat(columnList.getDataTypes()).isEqualTo(dataTypes);
    }

    @Test
    void testWithNamesOnly() {
        final List<String> names = List.of("a", "b", "c");

        final ColumnList columnList = ColumnList.of(names);

        assertThat(columnList.getNames()).isEqualTo(names);
        assertThat(columnList.getDataTypes()).isEmpty();
    }

    @Test
    void testWithEmptyLists() {
        final ColumnList columnList = ColumnList.of(List.of(), List.of());

        assertThat(columnList.getNames()).isEmpty();
        assertThat(columnList.getDataTypes()).isEmpty();
    }

    @Test
    void testToStringWithNamesOnly() {
        final ColumnList columnList = ColumnList.of("a", "b", "c");

        assertThat(columnList.toString()).isEqualTo("(`a`, `b`, `c`)");
    }

    @Test
    void testToStringWithNamesAndDataTypes() {
        final List<String> names = List.of("a", "b", "c");
        final List<DataType> dataTypes =
                List.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN());

        final ColumnList columnList = ColumnList.of(names, dataTypes);

        assertThat(columnList.toString()).isEqualTo("(`a` INT, `b` STRING, `c` BOOLEAN)");
    }

    @Test
    void testEqualsAndHashCode() {
        final List<String> names = List.of("a", "b");
        final List<DataType> dataTypes = List.of(DataTypes.INT(), DataTypes.STRING());

        final ColumnList columnList1 = ColumnList.of(names, dataTypes);
        final ColumnList columnList2 = ColumnList.of(names, dataTypes);
        final ColumnList columnList3 = ColumnList.of(List.of("a", "b"));

        assertThat(columnList1).isEqualTo(columnList2);
        assertThat(columnList1.hashCode()).isEqualTo(columnList2.hashCode());
        assertThat(columnList1).isNotEqualTo(columnList3);
    }

    @Test
    void testNullNamesThrowsException() {
        assertThatThrownBy(() -> ColumnList.of(null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Names must not be null");
    }

    @Test
    void testNullDataTypesThrowsException() {
        assertThatThrownBy(() -> ColumnList.of(List.of("a"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Data types must not be null");
    }

    @Test
    void testMismatchedDataTypesAndNamesThrowsException() {
        final List<String> names = List.of("a", "b", "c");
        final List<DataType> dataTypes = List.of(DataTypes.INT(), DataTypes.STRING());

        assertThatThrownBy(() -> ColumnList.of(names, dataTypes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Mismatch between data types and names");
    }

    @Test
    void testNullElementInNamesThrowsException() {
        final List<String> names = Arrays.asList("a", null, "c");

        assertThatThrownBy(() -> ColumnList.of(names))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Names must not contain null elements");
    }

    @Test
    void testNullElementInDataTypesThrowsException() {
        final List<String> names = List.of("a", "b", "c");
        final List<DataType> dataTypes = Arrays.asList(DataTypes.INT(), null, DataTypes.BOOLEAN());

        assertThatThrownBy(() -> ColumnList.of(names, dataTypes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Data types must not contain null elements");
    }

    @Test
    void testImmutabilityOfNames() {
        final List<String> names = new ArrayList<>(List.of("a", "b"));
        final ColumnList columnList = ColumnList.of(names);

        names.add("c");

        assertThat(columnList.getNames()).containsExactly("a", "b");
    }

    @Test
    void testImmutabilityOfDataTypes() {
        final List<String> names = new ArrayList<>(List.of("a", "b"));
        final List<DataType> dataTypes =
                new ArrayList<>(List.of(DataTypes.INT(), DataTypes.STRING()));
        final ColumnList columnList = ColumnList.of(names, dataTypes);

        dataTypes.add(DataTypes.BOOLEAN());

        assertThat(columnList.getDataTypes()).hasSize(2);
    }
}
