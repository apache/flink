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

package org.apache.flink.table.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.STRUCTURED;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DataType}. */
class DataTypeTest {

    @Test
    void testNullability() {
        assertThat(BIGINT().nullable()).isNullable();
        assertThat(BIGINT().notNull()).isNotNullable();
        assertThat(BIGINT().notNull().nullable()).isNullable();
    }

    @Test
    void testAtomicConversion() {
        assertThat(TIMESTAMP(0).bridgedTo(java.sql.Timestamp.class))
                .hasConversionClass(java.sql.Timestamp.class);
    }

    @Test
    void testTolerantAtomicConversion() {
        // this is logically only supported as input type because of
        // nullability but is tolerated until the planner complains
        // about an output type
        assertThat(BIGINT().nullable().bridgedTo(long.class)).hasConversionClass(long.class);
    }

    @Test
    void testInvalidAtomicConversion() {
        assertThatThrownBy(() -> TIMESTAMP(0).bridgedTo(DataTypesTest.class))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testArrayElementConversion() {
        assertThat(ARRAY(ARRAY(INT().notNull().bridgedTo(int.class))))
                .hasConversionClass(int[][].class);
    }

    @Test
    void testTolerantArrayConversion() {
        // this is logically only supported as input type because of
        // nullability but is tolerated until the planner complains
        // about an output type
        assertThat(ARRAY(ARRAY(INT().nullable())).bridgedTo(int[][].class))
                .hasConversionClass(int[][].class);
    }

    @Test
    void testInvalidArrayConversion() {
        assertThatThrownBy(() -> ARRAY(ARRAY(INT())).bridgedTo(int[][][].class))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testTolerantMapConversion() {
        // this doesn't make much sense logically but is supported until the planner complains
        assertThat(MULTISET(MULTISET(INT().bridgedTo(int.class)))).hasConversionClass(Map.class);
    }

    @Test
    void testFields() {
        assertThat(ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN())))
                .getChildren()
                .containsExactly(CHAR(2), BOOLEAN());
    }

    @Test
    void testInvalidOrderInterval() {
        assertThatThrownBy(() -> INTERVAL(MONTH(), YEAR(2)))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testConversionEquality() {
        assertThat(DataTypes.VARCHAR(2).bridgedTo(String.class)).isEqualTo(DataTypes.VARCHAR(2));
    }

    @Test
    void testArrayInternalElementConversion() {
        assertThat(ARRAY(STRING()).bridgedTo(ArrayData.class))
                .getChildren()
                .containsExactly(STRING().bridgedTo(StringData.class));
    }

    @Test
    void testMapInternalElementConversion() {
        assertThat(MAP(STRING(), ROW()).bridgedTo(MapData.class))
                .getChildren()
                .containsExactly(
                        STRING().bridgedTo(StringData.class), ROW().bridgedTo(RowData.class));
    }

    @Test
    void testGetFieldNames() {
        assertThat(
                        DataType.getFieldNames(
                                ROW(
                                        FIELD("c0", BOOLEAN()),
                                        FIELD("c1", DOUBLE()),
                                        FIELD("c2", INT()))))
                .containsExactly("c0", "c1", "c2");
        assertThat(
                        DataType.getFieldNames(
                                STRUCTURED(
                                        DataTypesTest.SimplePojo.class,
                                        FIELD("name", STRING()),
                                        FIELD("count", INT().notNull().bridgedTo(int.class)))))
                .containsExactly("name", "count");
        assertThat(DataType.getFieldNames(ARRAY(INT()))).isEmpty();
        assertThat(DataType.getFieldNames(INT())).isEmpty();
    }

    @Test
    void testGetFieldDataTypes() {
        assertThat(
                        DataType.getFieldDataTypes(
                                ROW(
                                        FIELD("c0", BOOLEAN()),
                                        FIELD("c1", DOUBLE()),
                                        FIELD("c2", INT()))))
                .containsExactly(BOOLEAN(), DOUBLE(), INT());
        assertThat(
                        DataType.getFieldDataTypes(
                                STRUCTURED(
                                        DataTypesTest.SimplePojo.class,
                                        FIELD("name", STRING()),
                                        FIELD("count", INT().notNull().bridgedTo(int.class)))))
                .containsExactly(STRING(), INT().notNull().bridgedTo(int.class));
        assertThat(DataType.getFieldDataTypes(ARRAY(INT()))).isEmpty();
        assertThat(DataType.getFieldDataTypes(INT())).isEmpty();
    }

    @Test
    void testGetFieldCount() {
        assertThat(
                        DataType.getFieldCount(
                                ROW(
                                        FIELD("c0", BOOLEAN()),
                                        FIELD("c1", DOUBLE()),
                                        FIELD("c2", INT()))))
                .isEqualTo(3);
        assertThat(
                        DataType.getFieldCount(
                                STRUCTURED(
                                        DataTypesTest.SimplePojo.class,
                                        FIELD("name", STRING()),
                                        FIELD("count", INT().notNull().bridgedTo(int.class)))))
                .isEqualTo(2);
        assertThat(DataType.getFieldCount(ARRAY(INT()))).isZero();
        assertThat(DataType.getFieldCount(INT())).isZero();
    }

    @Test
    void testGetFields() {
        assertThat(
                        DataType.getFields(
                                ROW(
                                        FIELD("c0", BOOLEAN()),
                                        FIELD("c1", DOUBLE()),
                                        FIELD("c2", INT()))))
                .containsExactly(FIELD("c0", BOOLEAN()), FIELD("c1", DOUBLE()), FIELD("c2", INT()));
        assertThat(
                        DataType.getFields(
                                STRUCTURED(
                                        DataTypesTest.SimplePojo.class,
                                        FIELD("name", STRING()),
                                        FIELD("count", INT().notNull().bridgedTo(int.class)))))
                .containsExactly(
                        FIELD("name", STRING()),
                        FIELD("count", INT().notNull().bridgedTo(int.class)));
        assertThat(DataType.getFields(ARRAY(INT()))).isEmpty();
        assertThat(DataType.getFields(INT())).isEmpty();
    }

    @Test
    void testArrayConversionClass() {
        assertThat(DataTypes.ARRAY(INT())).hasConversionClass(Integer[].class);
        assertThat(DataTypes.ARRAY(INT().notNull())).hasConversionClass(int[].class);
        DataType type = DataTypes.ARRAY(INT());
        assertThat(DataTypeUtils.transform(type, TypeTransformations.toNullable()))
                .hasConversionClass(Integer[].class);
        type = DataTypes.ARRAY(INT()).bridgedTo(int[].class);
        assertThat(DataTypeUtils.transform(type, TypeTransformations.toNullable()))
                .hasConversionClass(int[].class);
    }
}
