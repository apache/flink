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

package org.apache.flink.table.utils.python;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PythonTableUtilsTest {

    @Test
    void testCreateLiteralConvertsPy4JNumbers() {
        assertThat(literalValue(1, DataTypes.TINYINT().notNull())).isInstanceOf(Byte.class);
        assertThat(literalValue(1, DataTypes.SMALLINT().notNull())).isInstanceOf(Short.class);
        assertThat(literalValue(1, DataTypes.BIGINT().notNull())).isInstanceOf(Long.class);
        assertThat(literalValue(1.25, DataTypes.FLOAT().notNull())).isInstanceOf(Float.class);
        assertThat(
                        literalValue(
                                14,
                                DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()).notNull()))
                .isEqualTo(Period.ofMonths(14));
    }

    @Test
    void testCreateLiteralRejectsIncompatibleNestedValue() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Collections.singletonList(1.25),
                                        DataTypes.ARRAY(DataTypes.SMALLINT()).notNull()))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testCreateLiteralRejectsNonNullMultiset() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Collections.singletonMap(1, 2),
                                        DataTypes.MULTISET(DataTypes.SMALLINT()).notNull()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Non-null MULTISET literals are not supported.");
    }

    @Test
    void testCreateLiteralSupportsTypedNull() {
        final DataType dataType = DataTypes.ARRAY(DataTypes.SMALLINT());
        final ValueLiteralExpression literal =
                (ValueLiteralExpression) PythonTableUtils.createLiteral(null, dataType).toExpr();

        assertThat(literal.getOutputDataType()).isEqualTo(dataType);
        assertThat(literal.getValueAs(Object.class)).isEmpty();
    }

    @Test
    void testCreateLiteralUsesJavaArrayInferenceRules() {
        assertThatThrownBy(() -> PythonTableUtils.createLiteral(Collections.emptyList(), null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(() -> PythonTableUtils.createLiteral(Arrays.asList("a", "bb"), null))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testCreateLiteralInfersNestedArraysFromSiblings() {
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(Arrays.asList(1), Collections.emptyList()), null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(Collections.emptyList(), Arrays.asList(1)), null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(Arrays.asList(1), Collections.singletonList(null)),
                                null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(
                                        Collections.singletonList(Collections.singletonList(1)),
                                        Collections.singletonList(Collections.emptyList())),
                                null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.<Object>asList(new Integer[] {1}, Collections.emptyList()),
                                null))
                .isNotNull();
    }

    @Test
    void testCreateLiteralInfersValueDependentNestedArraysFromSiblings() {
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(
                                        Collections.singletonList("a"), Collections.emptyList()),
                                null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(
                                        Collections.singletonList("a"),
                                        Collections.singletonList(null)),
                                null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(
                                        Collections.singletonList(new BigDecimal("1.20")),
                                        Collections.emptyList()),
                                null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(
                                        Collections.singletonList(
                                                LocalTime.of(12, 0, 0, 123_000_000)),
                                        Collections.emptyList()),
                                null))
                .isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(
                                        Collections.singletonList(new byte[] {1}),
                                        Collections.emptyList()),
                                null))
                .isNotNull();
    }

    @Test
    void testCreateLiteralSupportsPrimitiveArrays() {
        for (final Object value :
                Arrays.<Object>asList(
                        new boolean[] {true},
                        new short[] {1},
                        new int[] {1},
                        new long[] {1},
                        new float[] {1},
                        new double[] {1})) {
            assertThat(PythonTableUtils.createLiteral(value, null)).isNotNull();
        }
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(new int[] {1}, Collections.emptyList()), null))
                .isNotNull();
    }

    @Test
    void testCreateLiteralUsesInferredArrayTypeHint() {
        final Object inferredArrayValue =
                PythonTableUtils.createInferredArrayValue(
                        new String[0], DataTypes.ARRAY(DataTypes.CHAR(4)).notNull());

        assertThat(PythonTableUtils.createLiteral(inferredArrayValue, null)).isNotNull();
        assertThat(
                        PythonTableUtils.createLiteral(
                                Arrays.asList(inferredArrayValue, Collections.emptyList()), null))
                .isNotNull();
    }

    @Test
    void testCreateInferredArrayValueValidatesCarrier() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createInferredArrayValue(
                                        new String[] {"a"},
                                        DataTypes.ARRAY(DataTypes.CHAR(1)).notNull()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "An inferred array value requires an empty array matching the ARRAY "
                                + "conversion class.");
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createInferredArrayValue(
                                        new String[0], DataTypes.ARRAY(DataTypes.INT()).notNull()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "An inferred array value requires an empty array matching the ARRAY "
                                + "conversion class.");
    }

    @Test
    void testCreateLiteralRejectsArraysWithoutCommonSiblingType() {
        assertThatThrownBy(
                        () -> PythonTableUtils.createLiteral(Collections.singletonList(null), null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.asList(
                                                Collections.emptyList(),
                                                Collections.singletonList(null)),
                                        null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.asList(
                                                Collections.singletonList(1),
                                                Collections.singletonList(1L)),
                                        null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.asList(
                                                Collections.singletonList("a"),
                                                Collections.singletonList("bb")),
                                        null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.asList(1, Collections.emptyList()), null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.<Object>asList(
                                                new byte[] {1}, Collections.emptyList()),
                                        null))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.asList(
                                                Collections.singletonList(new byte[] {1}),
                                                Collections.singletonList(Collections.emptyList())),
                                        null))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testCreateLiteralRejectsBinaryAsConstructedValue() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        new byte[] {1},
                                        DataTypes.ARRAY(DataTypes.TINYINT()).notNull()))
                .isInstanceOf(ValidationException.class);
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        new byte[] {1, 2},
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("a", DataTypes.TINYINT()),
                                                        DataTypes.FIELD("b", DataTypes.TINYINT()))
                                                .notNull()))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testCreateLiteralRejectsEmptyRow() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Collections.emptyList(), DataTypes.ROW().notNull()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Non-null empty ROW literals are not supported.");
    }

    @Test
    void testCreateLiteralRejectsWrongRowArity() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Arrays.asList(1, 2),
                                        DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT()))
                                                .notNull()))
                .isInstanceOf(ValidationException.class)
                .hasMessage("ROW literal has arity 2 but the data type has arity 1.");
    }

    @Test
    void testCreateLiteralRejectsNonInsertRow() {
        assertThatThrownBy(
                        () ->
                                PythonTableUtils.createLiteral(
                                        Row.ofKind(RowKind.DELETE, 1),
                                        DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT()))
                                                .notNull()))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Unsupported kind 'DELETE' of a row [-D[1]]. "
                                + "Only rows with 'INSERT' kind are supported when converting "
                                + "to an expression.");
    }

    private static Object literalValue(final Object value, final DataType dataType) {
        final ValueLiteralExpression literal =
                (ValueLiteralExpression) PythonTableUtils.createLiteral(value, dataType).toExpr();
        return literal.getValueAs(Object.class).orElseThrow(AssertionError::new);
    }
}
