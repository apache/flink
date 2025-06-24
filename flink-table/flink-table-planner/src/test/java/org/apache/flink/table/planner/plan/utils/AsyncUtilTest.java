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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.planner.plan.utils.AsyncUtil.containsAsyncCall;
import static org.apache.flink.table.planner.plan.utils.AsyncUtil.containsNonAsyncCall;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AsyncUtil}. */
public class AsyncUtilTest {

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

        converter =
                ShortcutUtils.unwrapContext(
                                plannerMocks.getPlanner().createToRelContext().getCluster())
                        .getRexFactory()
                        .createSqlToRexConverter(tableRowType, null);

        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("asyncScalarFn", new AsyncFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("asyncTableFn", new AsyncTableFunc(), false);

        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("scalarFn", new ScalarFunc(), false);
        plannerMocks
                .getFunctionCatalog()
                .registerTemporarySystemFunction("tableFn", new TableFunc(), false);
    }

    @Test
    void testFindScalar() {
        assertThat(containsAsyncCall(convert("asyncScalarFn(1, 2, 'test')"))).isTrue();
        assertThat(containsNonAsyncCall(convert("asyncScalarFn(1, 2, 'test')"))).isFalse();
        assertThat(containsNonAsyncCall(convert("scalarFn(1, 2, 'test')"))).isTrue();
        assertThat(
                        containsAsyncCall(
                                convert("asyncScalarFn(1, 2, 'test')"), FunctionKind.ASYNC_SCALAR))
                .isTrue();
        assertThat(
                        containsAsyncCall(
                                convert("asyncScalarFn(1, 2, 'test')"), FunctionKind.ASYNC_TABLE))
                .isFalse();
        assertThat(containsAsyncCall(convert("scalarFn(1, 2, 'test')"), FunctionKind.ASYNC_SCALAR))
                .isFalse();
        assertThat(
                        containsAsyncCall(
                                convert("scalarFn(1, 2, asyncScalarFn(1, 2, 'test'))"),
                                FunctionKind.ASYNC_SCALAR))
                .isTrue();
        assertThat(containsNonAsyncCall(convert("scalarFn(1, 2, asyncScalarFn(1, 2, 'test'))")))
                .isTrue();
        assertThat(containsNonAsyncCall(convert("asyncScalarFn(1, 2, scalarFn(1, 2, 'test'))")))
                .isTrue();
        assertThat(containsAsyncCall(convert("CONCAT('a', asyncScalarFn(1, 2, 'test'))"))).isTrue();
    }

    @Test
    void testFindTable() {
        assertThat(containsAsyncCall(convert("asyncTableFn(1, 2, 'test')"))).isTrue();
        assertThat(containsNonAsyncCall(convert("asyncTableFn(1, 2, 'test')"))).isFalse();
        assertThat(containsNonAsyncCall(convert("tableFn(1, 2, 'test')"))).isTrue();
        assertThat(
                        containsAsyncCall(
                                convert("asyncTableFn(1, 2, 'test')"), FunctionKind.ASYNC_TABLE))
                .isTrue();
        assertThat(
                        containsAsyncCall(
                                convert("asyncTableFn(1, 2, 'test')"), FunctionKind.ASYNC_SCALAR))
                .isFalse();
        assertThat(containsAsyncCall(convert("tableFn(1, 2, 'test')"), FunctionKind.ASYNC_TABLE))
                .isFalse();
    }

    private RexNode convert(String sqlExpression) {
        return converter.convertToRexNode(sqlExpression);
    }

    /** Test function. */
    public static final class AsyncFunc extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> f, Integer i, Long l, String s) {}
    }

    /** Test function. */
    public static final class AsyncTableFunc extends AsyncTableFunction<String> {
        public void eval(CompletableFuture<Collection<String>> f, Integer i, Long l, String s) {}
    }

    /** Test function. */
    public static final class ScalarFunc extends ScalarFunction {
        public String eval(Integer i, Long l, String s) {
            return null;
        }
    }

    /** Test function. */
    public static final class TableFunc extends TableFunction<String> {
        public String eval(Integer i, Long l, String s) {
            return null;
        }
    }
}
