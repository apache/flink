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
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AsyncCodeGenerator}. */
public class AsyncCodeGeneratorTest {

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
        converter =
                ShortcutUtils.unwrapContext(
                                plannerMocks.getPlanner().createToRelContext().getCluster())
                        .getRexFactory()
                        .createSqlToRexConverter(tableRowType, null);

        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc", new AsyncFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc_error", new AsyncFuncError(), false);
    }

    @Test
    public void testStringReturnType() throws Exception {
        RowData rowData =
                execute(
                        "myFunc(f1, f2, f3)",
                        RowType.of(new VarCharType()),
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(rowData).isEqualTo(GenericRowData.of(StringData.fromString("complete foo 4 6")));
    }

    @Test
    public void testTwoReturnTypes_passThroughFirst() throws Exception {
        RowData rowData =
                execute(
                        Arrays.asList("f2", "myFunc(f1, f2, f3)"),
                        RowType.of(new VarCharType(), new BigIntType()),
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(rowData)
                .isEqualTo(GenericRowData.of(3L, StringData.fromString("complete foo 4 6")));
    }

    @Test
    public void testTwoReturnTypes_passThroughSecond() throws Exception {
        RowData rowData =
                execute(
                        Arrays.asList("myFunc(f1, f2, f3)", "f2"),
                        RowType.of(new VarCharType(), new BigIntType()),
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(rowData)
                .isEqualTo(GenericRowData.of(StringData.fromString("complete foo 4 6"), 3L));
    }

    @Test
    public void testError() throws Exception {
        CompletableFuture<Collection<RowData>> future =
                executeFuture(
                        Arrays.asList("myFunc_error(f1, f2, f3)"),
                        RowType.of(new VarCharType(), new BigIntType()),
                        GenericRowData.of(2, 3L, StringData.fromString("foo")));
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get).cause().hasMessage("Error!");
    }

    private RowData execute(String sqlExpression, RowType resultType, RowData input)
            throws Exception {
        return execute(Arrays.asList(sqlExpression), resultType, input);
    }

    private RowData execute(List<String> sqlExpressions, RowType resultType, RowData input)
            throws Exception {
        Collection<RowData> result = executeFuture(sqlExpressions, resultType, input).get();
        assertThat(result).hasSize(1);
        return result.iterator().next();
    }

    private CompletableFuture<Collection<RowData>> executeFuture(
            List<String> sqlExpressions, RowType resultType, RowData input) throws Exception {
        List<RexNode> nodes =
                sqlExpressions.stream()
                        .map(sql -> converter.convertToRexNode(sql))
                        .collect(Collectors.toList());
        GeneratedFunction<AsyncFunction<RowData, RowData>> function =
                AsyncCodeGenerator.generateFunction(
                        "name",
                        INPUT_TYPE,
                        resultType,
                        nodes,
                        true,
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());
        AsyncFunction<RowData, RowData> asyncFunction =
                function.newInstance(Thread.currentThread().getContextClassLoader());
        TestResultFuture resultFuture = new TestResultFuture();
        asyncFunction.asyncInvoke(input, resultFuture);
        return resultFuture.getResult();
    }

    /** Test function. */
    public static final class AsyncFunc extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, Integer i, Long l, String s) {
            f.complete("complete " + s + " " + (i * i) + " " + (2 * l));
        }
    }

    /** Test function. */
    public static final class AsyncFuncError extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, Integer i, Long l, String s) {
            f.completeExceptionally(new RuntimeException("Error!"));
        }
    }

    /** Test result future. */
    public static final class TestResultFuture implements ResultFuture<RowData> {

        CompletableFuture<Collection<RowData>> data = new CompletableFuture<>();

        @Override
        public void complete(Collection<RowData> result) {
            data.complete(result);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            data.completeExceptionally(error);
        }

        public CompletableFuture<Collection<RowData>> getResult() {
            return data;
        }
    }
}
