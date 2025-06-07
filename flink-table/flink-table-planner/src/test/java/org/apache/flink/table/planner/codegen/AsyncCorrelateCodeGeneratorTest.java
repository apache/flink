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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AsyncCorrelateCodeGenerator}. */
public class AsyncCorrelateCodeGeneratorTest {

    private static final RowType INPUT_TYPE =
            RowType.of(new IntType(), new BigIntType(), new VarCharType());

    private PlannerMocks plannerMocks;
    private SqlToRexConverter converter;

    private RelDataType tableRowType;

    @BeforeEach
    public void before() {
        plannerMocks = PlannerMocks.create();
        tableRowType =
                plannerMocks
                        .getPlannerContext()
                        .getTypeFactory()
                        .buildRelNodeRowType(
                                JavaScalaConversionUtil.toScala(Arrays.asList("f1", "f2", "f3")),
                                JavaScalaConversionUtil.toScala(
                                        Arrays.asList(
                                                new IntType(),
                                                new BigIntType(),
                                                new VarCharType())));
        ShortcutUtils.unwrapContext(plannerMocks.getPlanner().createToRelContext().getCluster());
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc", new AsyncFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc2", new AsyncRowFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc3", new AsyncRowDataFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc_error", new AsyncFuncError(), false);

        converter =
                ShortcutUtils.unwrapContext(
                                plannerMocks.getPlanner().createToRelContext().getCluster())
                        .getRexFactory()
                        .createSqlToRexConverter(
                                tableRowType,
                                plannerMocks
                                        .getPlannerContext()
                                        .getTypeFactory()
                                        .createFieldTypeFromLogicalType(
                                                RowType.of(VarCharType.STRING_TYPE)));
    }

    @Test
    public void testStringReturnType() throws Exception {
        List<Object> objects =
                execute(
                        "myFunc(f1, f2, f3)",
                        RowType.of(VarCharType.STRING_TYPE),
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(objects)
                .containsExactly(Row.of("complete1 foo 4 6"), Row.of("complete2 foo 4 6"));
    }

    @Test
    public void testRowReturnType() throws Exception {
        RowType type = RowType.of(new IntType(), new BigIntType(), VarCharType.STRING_TYPE);
        List<Object> objects =
                execute(
                        "myFunc2(f1, f2, f3)",
                        type,
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(objects)
                .containsExactly(Row.of(2, 30L, "complete1 foo"), Row.of(2, 60L, "complete2 foo"));

        objects =
                execute(
                        "myFunc2(f1, f2, f3)",
                        type,
                        GenericRowData.of(0, 3L, StringData.fromString("foo")));
        assertThat(objects).containsExactly();

        objects =
                execute(
                        "myFunc2(f1, f2, f3)",
                        type,
                        GenericRowData.of(1, 3L, StringData.fromString("foo")));
        assertThat(objects).containsExactly(Row.of(1, 30L, "complete foo"));
    }

    @Test
    public void testRowDataReturnType() throws Exception {
        RowType type = RowType.of(new IntType(), new BigIntType(), VarCharType.STRING_TYPE);
        List<Object> objects =
                execute(
                        "myFunc3(f1, f2, f3)",
                        type,
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(objects)
                .containsExactly(
                        GenericRowData.of(2, 30L, "complete1 foo"),
                        GenericRowData.of(2, 60L, "complete2 foo"));

        objects =
                execute(
                        "myFunc3(f1, f2, f3)",
                        type,
                        GenericRowData.of(0, 3L, StringData.fromString("foo")));
        assertThat(objects).containsExactly();

        objects =
                execute(
                        "myFunc3(f1, f2, f3)",
                        type,
                        GenericRowData.of(1, 3L, StringData.fromString("foo")));
        assertThat(objects).containsExactly(GenericRowData.of(1, 30L, "complete foo"));
    }

    @Test
    public void testError() throws Exception {
        CompletableFuture<Collection<Object>> future =
                executeFuture(
                        "myFunc_error(f1, f2, f3)",
                        RowType.of(VarCharType.STRING_TYPE),
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get).cause().hasMessage("Error!");
    }

    private List<Object> execute(String sqlExpression, RowType resultType, RowData input)
            throws Exception {
        Collection<Object> result = executeFuture(sqlExpression, resultType, input).get();
        return new ArrayList<>(result);
    }

    private CompletableFuture<Collection<Object>> executeFuture(
            String sqlExpression, RowType resultType, RowData input) throws Exception {
        RelDataType type =
                plannerMocks.getPlannerContext().getTypeFactory().buildRelNodeRowType(resultType);

        RexCall node = (RexCall) converter.convertToRexNode(sqlExpression);
        node = node.clone(type, node.getOperands());

        GeneratedFunction<AsyncFunction<RowData, Object>> function =
                AsyncCorrelateCodeGenerator.generateFunction(
                        "name",
                        INPUT_TYPE,
                        resultType,
                        node,
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());
        AsyncFunction<RowData, Object> asyncFunction =
                function.newInstance(Thread.currentThread().getContextClassLoader());
        TestResultFuture resultFuture = new TestResultFuture();
        asyncFunction.asyncInvoke(input, resultFuture);
        return resultFuture.getResult();
    }

    /** Test function. */
    public static final class AsyncFunc extends AsyncTableFunction<String> {
        public void eval(CompletableFuture<Collection<String>> f, Integer i, Long l, String s) {
            f.complete(
                    Arrays.asList(
                            "complete1 " + s + " " + (i * i) + " " + (2 * l),
                            "complete2 " + s + " " + (i * i) + " " + (2 * l)));
        }
    }

    /** Test function. */
    @FunctionHint(output = @DataTypeHint("ROW<i INT, b BIGINT, s STRING>"))
    public static final class AsyncRowFunc extends AsyncTableFunction<Row> {
        public void eval(CompletableFuture<Collection<Row>> f, Integer i, Long l, String s) {
            if (i == 0) {
                f.complete(Collections.emptyList());
            } else if (i == 1) {
                f.complete(Collections.singletonList(Row.of(i, l * 10, "complete " + s)));
            } else {
                f.complete(
                        Arrays.asList(
                                Row.of(i, l * 10, "complete1 " + s),
                                Row.of(i, l * 20, "complete2 " + s)));
            }
        }
    }

    /** Test function. */
    @FunctionHint(
            output =
                    @DataTypeHint(
                            value = "ROW<i INT, b BIGINT, s STRING>",
                            bridgedTo = RowData.class))
    public static final class AsyncRowDataFunc extends AsyncTableFunction<RowData> {
        public void eval(CompletableFuture<Collection<RowData>> f, Integer i, Long l, String s) {
            if (i == 0) {
                f.complete(Collections.emptyList());
            } else if (i == 1) {
                f.complete(
                        Collections.singletonList(GenericRowData.of(i, l * 10, "complete " + s)));
            } else {
                f.complete(
                        Arrays.asList(
                                GenericRowData.of(i, l * 10, "complete1 " + s),
                                GenericRowData.of(i, l * 20, "complete2 " + s)));
            }
        }
    }

    /** Test function. */
    public static final class AsyncFuncError extends AsyncTableFunction<String> {
        public void eval(CompletableFuture<Collection<String>> f, Integer i, Long l, String s) {
            f.completeExceptionally(new RuntimeException("Error!"));
        }
    }

    /** Test result future. */
    public static final class TestResultFuture implements ResultFuture<Object> {

        CompletableFuture<Collection<Object>> data = new CompletableFuture<>();

        @Override
        public void complete(Collection<Object> result) {
            data.complete(result);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            data.completeExceptionally(error);
        }

        public CompletableFuture<Collection<Object>> getResult() {
            return data;
        }

        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            try {
                data.complete(supplier.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
