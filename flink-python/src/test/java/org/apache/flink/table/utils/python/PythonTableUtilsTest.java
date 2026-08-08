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

import org.junit.jupiter.api.Test;

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

    private static Object literalValue(final Object value, final DataType dataType) {
        final ValueLiteralExpression literal =
                (ValueLiteralExpression) PythonTableUtils.createLiteral(value, dataType).toExpr();
        return literal.getValueAs(Object.class).orElseThrow(AssertionError::new);
    }
}
