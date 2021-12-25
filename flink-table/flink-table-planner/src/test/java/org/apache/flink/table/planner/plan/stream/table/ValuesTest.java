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

package org.apache.flink.table.planner.plan.stream.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.pi;
import static org.apache.flink.table.api.Expressions.row;

/** Tests for {@link org.apache.flink.table.api.TableEnvironment#fromValues}. */
public class ValuesTest extends TableTestBase {

    @Test
    public void testValuesAllEqualTypes() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                row(1, 2L, "JKL"),
                                row(2, 3L, "GHI"),
                                row(3, 4L, "DEF"),
                                row(4, 5L, "ABC"));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesFromLiterals() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t = util.getTableEnv().fromValues(1, 3.1f, 99L, null);
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesFromRowExpression() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                row(lit(1).plus(3), "ABC", map("a", 3d)),
                                row(lit(-1).abs().plus(2), "ABC", map("a", lit(-5).abs().plus(-5))),
                                row(pi(), "ABC", map("abc", 3f)),
                                row(3.1f, "DEF", map("abcd", 3L)),
                                row(99L, "DEFG", map("a", 1)),
                                row(
                                        0d,
                                        "D",
                                        lit(
                                                null,
                                                DataTypes.MAP(
                                                        DataTypes.CHAR(1), DataTypes.INT()))));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesFromRowObject() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                Row.of(1, "ABC", null),
                                Row.of(Math.PI, "ABC", 1),
                                Row.of(3.1f, "DEF", 2),
                                Row.of(99L, "DEFG", 3),
                                Row.of(0d, "D", 4));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesFromMixedObjectsAndExpressions() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                row(1, "ABC", null),
                                Row.of(Math.PI, "ABC", 1),
                                Row.of(3.1f, "DEF", 2),
                                row(99L, "DEFG", nullOf(DataTypes.INT())),
                                Row.of(0d, "D", 4));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesFromRowObjectInCollection() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        List<Object> data =
                Arrays.asList(
                        row(1, lit("ABC")),
                        row(Math.PI, "ABC"),
                        row(3.1f, "DEF"),
                        row(99L, lit("DEFG")),
                        row(0d, "D"));

        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.DECIMAL(10, 2).notNull()),
                        DataTypes.FIELD("b", DataTypes.CHAR(4).notNull()));

        Table t = util.getTableEnv().fromValues(rowType, data);
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesFromNestedRowObject() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                Row.of(1, Row.of("A", 2), singletonList(1)),
                                Row.of(Math.PI, Row.of("ABC", 3.0), singletonList(3L)));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesOverrideSchema() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.BIGINT()),
                                        DataTypes.FIELD("b", DataTypes.STRING())),
                                row(lit(1).plus(2), "ABC"),
                                row(2, "ABC"));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesOverrideNullability() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                DataTypes.ROW(
                                        DataTypes.FIELD("a", DataTypes.BIGINT().notNull()),
                                        DataTypes.FIELD("b", DataTypes.VARCHAR(4).notNull()),
                                        DataTypes.FIELD("c", DataTypes.BINARY(4).notNull())),
                                row(lit(1).plus(2), "ABC", new byte[] {1, 2, 3}));
        util.verifyExecPlan(t);
    }

    @Test
    public void testValuesWithComplexNesting() {
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        Table t =
                util.getTableEnv()
                        .fromValues(
                                DataTypes.ROW(
                                        DataTypes.FIELD("number", DataTypes.DOUBLE()),
                                        DataTypes.FIELD(
                                                "row",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "string", DataTypes.CHAR(5)),
                                                        DataTypes.FIELD(
                                                                "decimal",
                                                                DataTypes.DECIMAL(10, 2)),
                                                        DataTypes.FIELD(
                                                                "nestedRow",
                                                                DataTypes.ROW(
                                                                        DataTypes.FIELD(
                                                                                "time",
                                                                                DataTypes.TIME(
                                                                                        4)))))),
                                        DataTypes.FIELD(
                                                "array", DataTypes.ARRAY(DataTypes.BIGINT()))),
                                Row.of(
                                        1,
                                        Row.of("A", 2, Row.of(LocalTime.of(0, 0, 0))),
                                        singletonList(1)),
                                Row.of(
                                        Math.PI,
                                        Row.of(
                                                "ABC",
                                                3.0,
                                                Row.of(100 /* uses integer for a TIME(4)*/)),
                                        singletonList(3L)));
        util.verifyExecPlan(t);
    }

    @Test
    public void testNoCommonType() {
        thrown().expect(ValidationException.class);
        thrown().expectMessage(
                        "Types in fromValues(...) must have a common super type. Could not"
                                + " find a common type for all rows at column 1.");
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        util.getTableEnv()
                .fromValues(row("ABC", 1L), row("ABC", lit(LocalTime.of(0, 0, 0))), row("ABC", 2));
    }

    @Test
    public void testCannotCast() {
        thrown().expect(ValidationException.class);
        thrown().expectMessage(
                        "Could not cast the value of the 0 column: [ 4 ] of a row: [ 4 ]"
                                + " to the requested type: BINARY(3)");
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        util.getTableEnv()
                .fromValues(DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.BINARY(3))), row(4));
    }

    @Test
    public void testWrongRowTypeLength() {
        thrown().expect(ValidationException.class);
        thrown().expectMessage(
                        "All rows in a fromValues(...) clause must have the same fields number. Row [4] has a different"
                                + " length than the expected size: 2.");
        JavaStreamTableTestUtil util = javaStreamTestUtil();
        util.getTableEnv()
                .fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("f1", DataTypes.BINARY(3)),
                                DataTypes.FIELD("f2", DataTypes.STRING())),
                        row(4));
    }
}
