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
import org.apache.flink.types.RowKind;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AsyncCodeGenerator}. */
public class AsyncCodeGeneratorTest {

    private static final RowType INPUT_TYPE =
            RowType.of(new IntType(), new BigIntType(), new VarCharType());
    private static final RowType INPUT_TYPE2 =
            RowType.of(new VarCharType(), new VarCharType(), new VarCharType());

    private PlannerMocks plannerMocks;
    private SqlToRexConverter converter;
    private SqlToRexConverter converter2;

    private RelDataType tableRowType;
    private RelDataType tableRowType2;

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
        tableRowType2 =
                plannerMocks
                        .getPlannerContext()
                        .getTypeFactory()
                        .buildRelNodeRowType(
                                JavaScalaConversionUtil.toScala(Arrays.asList("f1", "f2", "f3")),
                                JavaScalaConversionUtil.toScala(
                                        Arrays.asList(
                                                new VarCharType(),
                                                new VarCharType(),
                                                new VarCharType())));
        ShortcutUtils.unwrapContext(plannerMocks.getPlanner().createToRelContext().getCluster());
        converter =
                ShortcutUtils.unwrapContext(
                                plannerMocks.getPlanner().createToRelContext().getCluster())
                        .getRexFactory()
                        .createSqlToRexConverter(tableRowType, null);
        converter2 =
                ShortcutUtils.unwrapContext(
                                plannerMocks.getPlanner().createToRelContext().getCluster())
                        .getRexFactory()
                        .createSqlToRexConverter(tableRowType2, null);

        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc", new AsyncFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("myfunc2", new AsyncFuncThreeParams(), false);
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
    public void testTwoReturnTypes_passThroughFirst_stringArgs() throws Exception {
        List<RowData> rowData =
                executeMany(
                        converter2,
                        INPUT_TYPE2,
                        Arrays.asList("f1", "myFunc2(f1, f2, f3)"),
                        RowType.of(new VarCharType(), new VarCharType()),
                        Arrays.asList(
                                GenericRowData.of(
                                        StringData.fromString("a1"),
                                        StringData.fromString("b1"),
                                        StringData.fromString("c1")),
                                GenericRowData.of(
                                        StringData.fromString("a2"),
                                        StringData.fromString("b2"),
                                        StringData.fromString("c2"))));
        assertThat(rowData.get(0))
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("a1"), StringData.fromString("val a1b1c1")));
        assertThat(rowData.get(1))
                .isEqualTo(
                        GenericRowData.of(
                                StringData.fromString("a2"), StringData.fromString("val a2b2c2")));
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
        List<CompletableFuture<Collection<RowData>>> futures =
                executeFuture(
                        converter,
                        INPUT_TYPE,
                        Arrays.asList("myFunc_error(f1, f2, f3)"),
                        RowType.of(new VarCharType(), new BigIntType()),
                        Arrays.asList(GenericRowData.of(2, 3L, StringData.fromString("foo"))));
        CompletableFuture<Collection<RowData>> future = futures.get(0);
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get).cause().hasMessage("Error!");
    }

    @Test
    public void testPassThroughChangelogTypes() throws Exception {
        RowData rowData =
                execute(
                        Arrays.asList("myFunc(f1)"),
                        RowType.of(new IntType()),
                        GenericRowData.ofKind(RowKind.INSERT, 2, 3L, StringData.fromString("foo")));
        assertThat(rowData).isEqualTo(GenericRowData.of(12));
        RowData rowData2 =
                execute(
                        Arrays.asList("myFunc(f1)"),
                        RowType.of(new IntType()),
                        GenericRowData.ofKind(
                                RowKind.UPDATE_AFTER, 2, 3L, StringData.fromString("foo")));
        assertThat(rowData2).isEqualTo(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 12));

        RowData rowData3 =
                execute(
                        Arrays.asList("myFunc(f1)"),
                        RowType.of(new IntType()),
                        GenericRowData.ofKind(RowKind.DELETE, 2, 3L, StringData.fromString("foo")));
        assertThat(rowData3).isEqualTo(GenericRowData.ofKind(RowKind.DELETE, 12));
    }

    private RowData execute(String sqlExpression, RowType resultType, RowData input)
            throws Exception {
        return execute(Arrays.asList(sqlExpression), resultType, input);
    }

    private RowData execute(List<String> sqlExpressions, RowType resultType, RowData input)
            throws Exception {
        Collection<RowData> result =
                executeFuture(
                                converter,
                                INPUT_TYPE,
                                sqlExpressions,
                                resultType,
                                Collections.singletonList(input))
                        .get(0)
                        .get();
        assertThat(result).hasSize(1);
        return result.iterator().next();
    }

    private List<RowData> executeMany(
            SqlToRexConverter converter,
            RowType rowType,
            List<String> sqlExpressions,
            RowType resultType,
            List<RowData> inputs)
            throws Exception {
        List<CompletableFuture<Collection<RowData>>> list =
                executeFuture(converter, rowType, sqlExpressions, resultType, inputs);
        CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).get();
        List<Collection<RowData>> results =
                list.stream().map(CompletableFuture::join).collect(Collectors.toList());
        assertThat(results).hasSize(inputs.size());
        return results.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    private List<CompletableFuture<Collection<RowData>>> executeFuture(
            SqlToRexConverter converter,
            RowType rowType,
            List<String> sqlExpressions,
            RowType resultType,
            List<RowData> inputs)
            throws Exception {
        List<RexNode> nodes =
                sqlExpressions.stream()
                        .map(sql -> converter.convertToRexNode(sql))
                        .collect(Collectors.toList());
        GeneratedFunction<AsyncFunction<RowData, RowData>> function =
                AsyncCodeGenerator.generateFunction(
                        "name",
                        rowType,
                        resultType,
                        nodes,
                        true,
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());
        AsyncFunction<RowData, RowData> asyncFunction =
                function.newInstance(Thread.currentThread().getContextClassLoader());
        List<CompletableFuture<Collection<RowData>>> results = new ArrayList<>();
        for (RowData input : inputs) {
            TestResultFuture resultFuture = new TestResultFuture();
            asyncFunction.asyncInvoke(input, resultFuture);
            results.add(resultFuture.getResult());
        }
        return results;
    }

    /** Test function. */
    public static final class AsyncFunc extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, Integer i, Long l, String s) {
            f.complete("complete " + s + " " + (i * i) + " " + (2 * l));
        }

        public void eval(CompletableFuture<Integer> f, Integer i) {
            f.complete(i + 10);
        }
    }

    /** Test function. */
    public static final class AsyncFuncError extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, Integer i, Long l, String s) {
            f.completeExceptionally(new RuntimeException("Error!"));
        }
    }

    /** Test function. */
    public static class AsyncFuncThreeParams extends AsyncScalarFunction {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<String> future, String a, String b, String c) {
            future.complete("val " + a + b + c);
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

        @Override
        public void complete(CollectionSupplier<RowData> supplier) {
            try {
                data.complete(supplier.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
