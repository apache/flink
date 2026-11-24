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

package org.apache.flink.table.planner.codegen.sort;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.GeneratedComparator;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ComparatorCodeGenerator#genComparator}. */
class ComparatorCodeGeneratorTest {

    @Test
    void testRowOrdersFieldByFieldWithNullFirst() {
        final GeneratedComparator comparator =
                open(
                        DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.INT()),
                                        DataTypes.FIELD("b", DataTypes.STRING()))
                                .toInternal());

        assertThat(comparator.compare(row(1, "b"), row(2, "a"))).isNegative();
        assertThat(comparator.compare(row(1, "b"), row(1, "a"))).isPositive();
        assertThat(comparator.compare(row(1, "a"), row(1, "a"))).isZero();
        assertThat(comparator.compare(row(null, "z"), row(0, "a"))).isNegative();
        assertThat(comparator.compare(row(1, null), row(1, "a"))).isNegative();
        assertThat(comparator.compare(row(null, null), row(null, null))).isZero();
    }

    @Test
    void testNonRowIsComparedAsRowOfOneField() {
        final GeneratedComparator comparator = open(DataTypes.BIGINT().toInternal());

        assertThat(comparator.compare(1L, 2L)).isNegative();
        assertThat(comparator.compare(2L, 1L)).isPositive();
        assertThat(comparator.compare(null, 1L)).isNegative();
    }

    @Test
    void testUnorderableTypeIsRejected() {
        assertThatThrownBy(
                        () ->
                                ComparatorCodeGenerator.genComparator(
                                        new Configuration(),
                                        Thread.currentThread().getContextClassLoader(),
                                        DataTypes.MAP(DataTypes.INT(), DataTypes.INT())
                                                .toInternal()))
                .isInstanceOf(Exception.class);
    }

    private static GeneratedComparator open(final DataType dataType) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final GeneratedComparator comparator =
                ComparatorCodeGenerator.genComparator(new Configuration(), classLoader, dataType);
        comparator.open(new FunctionContext(null, classLoader, null));
        return comparator;
    }

    private static GenericRowData row(final Integer a, final String b) {
        return GenericRowData.of(a, b == null ? null : StringData.fromString(b));
    }
}
