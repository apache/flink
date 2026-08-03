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
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PythonTableUtils}. */
class PythonTableUtilsTest {

    @Test
    void testCreateLiteralWithTypeConvertsPy4JNumbers() {
        assertThat(literalValue(1, DataTypes.TINYINT().notNull())).isInstanceOf(Byte.class);
        assertThat(literalValue(1, DataTypes.SMALLINT().notNull())).isInstanceOf(Short.class);
        assertThat(literalValue(1, DataTypes.BIGINT().notNull())).isInstanceOf(Long.class);
        assertThat(literalValue(1.25, DataTypes.FLOAT().notNull())).isInstanceOf(Float.class);
    }

    @Test
    void testCreateLiteralWithTypeConvertsCompositeValues() {
        Short[] array =
                (Short[])
                        literalValue(
                                Arrays.asList(1, 2),
                                DataTypes.ARRAY(DataTypes.SMALLINT()).notNull());
        assertThat(array).containsExactly((short) 1, (short) 2);

        Map<?, ?> map =
                (Map<?, ?>)
                        literalValue(
                                Collections.singletonMap(1, 1.25),
                                DataTypes.MAP(DataTypes.SMALLINT(), DataTypes.FLOAT()).notNull());
        assertThat(map.keySet()).allMatch(Short.class::isInstance);
        assertThat(map.values()).allMatch(Float.class::isInstance);

        Row row =
                (Row)
                        literalValue(
                                Arrays.asList(1, 1.25),
                                DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        "small_value", DataTypes.SMALLINT()),
                                                DataTypes.FIELD("float_value", DataTypes.FLOAT()))
                                        .notNull());
        assertThat(row.getField(0)).isInstanceOf(Short.class);
        assertThat(row.getField(1)).isInstanceOf(Float.class);
    }

    private static Object literalValue(Object value, DataType dataType) {
        ValueLiteralExpression literal =
                (ValueLiteralExpression)
                        PythonTableUtils.createLiteralWithType(value, dataType).toExpr();
        return literal.getValueAs(Object.class).orElseThrow(AssertionError::new);
    }
}
