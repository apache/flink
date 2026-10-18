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

package org.apache.flink.table.planner.runtime.batch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelAsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourcePythonScalarFunction;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.MockPythonTableFunction;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests batch rejection of concurrency and application of vectorized batch sizes. */
class PythonUdfExecutionOptionsTest extends TableTestBase {
    private BatchTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE src (a INT, b INT) WITH ("
                                + "'connector' = 'filesystem', 'format' = 'testcsv', 'path' = '/tmp')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE sink_table (a INT, b INT) "
                                + "WITH ('connector' = 'values')");
        util.addTemporarySystemFunction("p1", new ParallelPythonScalarFunction("p1", 1));
        util.addTemporarySystemFunction("p2", new ParallelPythonScalarFunction("p2", 2));
        util.addTemporarySystemFunction(
                "async_func", new ParallelAsyncPythonScalarFunction("async_func", 2));
        util.addTemporarySystemFunction("tf", new MockPythonTableFunction());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "SELECT a, p1(a, b) FROM src",
                "SELECT a, p2(a, b) FROM src",
                "SELECT p1(a, b), p2(a, b) FROM src",
                "SELECT a, p1(p2(a, b), b) FROM src",
                "SELECT a, async_func(a, b) FROM src",
                "SELECT x, y FROM src, LATERAL TABLE(tf(a, p2(a, b))) AS T(x, y)",
                "SELECT x, y FROM src, LATERAL TABLE(tf(p1(a, b), p2(a, b))) AS T(x, y)"
            })
    void testRejectsExplicitConcurrency(String query) {
        assertThatThrownBy(() -> translate(query))
                .hasMessageContaining("Python UDF concurrency is not supported in batch mode");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "pandas64(a, b), pandas32(a, b)",
                "a, pandas64(pandas32(a, b), b)",
                "a, pandas32(pandas64(a, b), b)"
            })
    void testMinimumBatchSizeOverridesDefault(String projection) throws Exception {
        util.getTableEnv().getConfig().set(PythonOptions.MAX_ARROW_BATCH_SIZE, 257);
        util.addTemporarySystemFunction(
                "pandas64",
                new ResourcePythonScalarFunction("pandas64", -1, PythonFunctionKind.PANDAS, 64));
        util.addTemporarySystemFunction(
                "pandas32",
                new ResourcePythonScalarFunction("pandas32", -1, PythonFunctionKind.PANDAS, 32));
        final OneInputTransformation<?, ?> python = translate("SELECT " + projection + " FROM src");
        assertThat(python.getParallelism()).isEqualTo(4);
        assertThat(python.isParallelismConfigured()).isFalse();
        assertThat(operatorConfig(python).get(PythonOptions.MAX_ARROW_BATCH_SIZE)).isEqualTo(32);
        assertThat(util.getTableEnv().getConfig().get(PythonOptions.MAX_ARROW_BATCH_SIZE))
                .isEqualTo(257);
    }

    @Test
    void testUnspecifiedOptionsInheritDefaults() throws Exception {
        util.getTableEnv().getConfig().set(PythonOptions.MAX_ARROW_BATCH_SIZE, 257);
        util.addTemporarySystemFunction(
                "plain",
                new ResourcePythonScalarFunction("plain", -1, PythonFunctionKind.PANDAS, -1));
        final OneInputTransformation<?, ?> python = translate("SELECT a, plain(a, b) FROM src");
        assertThat(python.getParallelism()).isEqualTo(4);
        assertThat(python.isParallelismConfigured()).isFalse();
        assertThat(operatorConfig(python).get(PythonOptions.MAX_ARROW_BATCH_SIZE)).isEqualTo(257);
    }

    private OneInputTransformation<?, ?> translate(String query) {
        return (OneInputTransformation<?, ?>)
                util
                        .getPlanner()
                        .translate(
                                Collections.singletonList(
                                        (ModifyOperation)
                                                util.getPlanner()
                                                        .getParser()
                                                        .parse("INSERT INTO sink_table " + query)
                                                        .get(0)))
                        .get(0)
                        .getTransitivePredecessors()
                        .stream()
                        .filter(node -> node.getName().startsWith("Python"))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing Python operator"));
    }

    private Configuration operatorConfig(OneInputTransformation<?, ?> transformation)
            throws Exception {
        final Object operator =
                ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
        Class<?> type = operator.getClass();
        while (type != null) {
            try {
                final Field field = type.getDeclaredField("config");
                field.setAccessible(true);
                return (Configuration) field.get(operator);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new AssertionError("Missing Python operator configuration");
    }
}
