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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParserException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for user defined functions in the Table API. */
public class FunctionITCase extends StreamingTestBase {

    @Test
    void testScalarFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(Row.of(1, 1L, 1L), Row.of(2, 2L, 1L), Row.of(3, 3L, 1L));

        final List<Row> sinkData =
                Arrays.asList(Row.of(1, 2L, 1L), Row.of(2, 4L, 1L), Row.of(3, 6L, 1L));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE TestTable(a INT, b BIGINT, c BIGINT) WITH ('connector' = 'COLLECTION')");

        final Table table =
                tEnv().from("TestTable")
                        .select(
                                $("a"),
                                call(new SimpleScalarFunction(), $("a"), $("b")),
                                call(new SimpleScalarFunction(), $("a"), $("b"))
                                        .plus(1)
                                        .minus(call(new SimpleScalarFunction(), $("a"), $("b"))));
        table.executeInsert("TestTable").await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    void testJoinWithTableFunction() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of("1,2,3"), Row.of("2,3,4"), Row.of("3,4,5"), Row.of((String) null));

        final List<Row> sinkData =
                Arrays.asList(
                        Row.of("1,2,3", new String[] {"1", "2", "3"}),
                        Row.of("2,3,4", new String[] {"2", "3", "4"}),
                        Row.of("3,4,5", new String[] {"3", "4", "5"}));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING>) WITH ('connector' = 'COLLECTION')");

        tEnv().from("SourceTable")
                .joinLateral(call(new SimpleTableFunction(), $("s")).as("a", "b"))
                .select($("a"), $("b"))
                .executeInsert("SinkTable")
                .await();

        assertThat(TestCollectionTableFactory.getResult()).isEqualTo(sinkData);
    }

    @Test
    void testLateralJoinWithScalarFunction() throws Exception {
        TestCollectionTableFactory.reset();
        tEnv().executeSql("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
        tEnv().executeSql(
                        "CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING>) WITH ('connector' = 'COLLECTION')");

        assertThatThrownBy(
                        () -> {
                            tEnv().from("SourceTable")
                                    .joinLateral(
                                            call(new RowScalarFunction(), $("s")).as("a", "b"));
                        })
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "A lateral join only accepts an expression which defines a table function"));
    }

    @Test
    public void testTableUdf() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> data = env.fromElements(Row.of(1), Row.of(2));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(data).as("f0");
        Double[][] d = new Double[][] {new Double[] {1.0, Double.NaN}};
        Expression[] expressions = new Expression[2];
        expressions[0] = org.apache.flink.table.api.Expressions.call(MyUDF.class, $("f0"), d);
        expressions[1] = org.apache.flink.table.api.Expressions.call(MyUDF.class, $("f0"), d);
        assertThatThrownBy(
                        () ->
                                table.addColumns(expressions)
                                        .as("f0", "output1", "output2")
                                        .execute())
                .satisfies(
                        anyCauseMatches(
                                ExpressionParserException.class,
                                "expression parse failed by trying to convert infinity/nan to Decimal"));

        Double[][] d1 = new Double[][] {new Double[] {1.0, Double.POSITIVE_INFINITY}};
        expressions[0] = org.apache.flink.table.api.Expressions.call(MyUDF.class, $("f0"), d1);
        expressions[1] = org.apache.flink.table.api.Expressions.call(MyUDF.class, $("f0"), d1);
        assertThatThrownBy(
                        () ->
                                table.addColumns(expressions)
                                        .as("f0", "output1", "output2")
                                        .execute())
                .satisfies(
                        anyCauseMatches(
                                ExpressionParserException.class,
                                "expression parse failed by trying to convert infinity/nan to Decimal"));

        Float[][] f = new Float[][] {new Float[] {1.0F, Float.NEGATIVE_INFINITY}};
        expressions[0] = org.apache.flink.table.api.Expressions.call(MyUDF1.class, $("f0"), f);
        expressions[1] = org.apache.flink.table.api.Expressions.call(MyUDF1.class, $("f0"), f);
        assertThatThrownBy(
                        () ->
                                table.addColumns(expressions)
                                        .as("f0", "output1", "output2")
                                        .execute())
                .satisfies(
                        anyCauseMatches(
                                ExpressionParserException.class,
                                "expression parse failed by trying to convert infinity/nan to Decimal"));
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    public static class MyUDF extends ScalarFunction {
        public Double eval(Double num, Double[][] add) {
            return num + add[0][0];
        }
    }

    public static class MyUDF1 extends ScalarFunction {
        public Float eval(Float num, Float[][] add) {
            return num + add[0][0];
        }
    }

    /** Simple scalar function. */
    public static class SimpleScalarFunction extends ScalarFunction {
        public Long eval(Integer i, Long j) {
            return i + j;
        }
    }

    /** Scalar function that returns a row. */
    @FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING>>"))
    public static class RowScalarFunction extends ScalarFunction {
        public Row eval(String s) {
            return Row.of(s, s.split(","));
        }
    }

    /** Table function that returns a row. */
    @FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING>>"))
    public static class SimpleTableFunction extends TableFunction<Row> {
        public void eval(String s) {
            if (s == null) {
                collect(null);
            } else {
                collect(Row.of(s, s.split(",")));
            }
        }
    }
}
