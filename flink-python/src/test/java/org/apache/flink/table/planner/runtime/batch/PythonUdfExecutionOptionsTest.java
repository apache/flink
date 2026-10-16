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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.functions.python.PythonScalarFunction;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PandasScalarFunction;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.runtime.operators.sort.LimitOperator;
import org.apache.flink.table.runtime.operators.sort.SortLimitOperator;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for applying per-UDF execution options to translated Python calc operators. */
class PythonUdfExecutionOptionsTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table (a INT, b INT) WITH ("
                                + "'connector' = 'filesystem', "
                                + "'format' = 'testcsv', "
                                + "'path' = '/tmp')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE sink_table (a INT, b INT) WITH ("
                                + "'connector' = 'values')");
    }

    @Test
    void testPandasOptionsSetParallelismAndMinimumBatchSize() throws Exception {
        util.addTemporarySystemFunction(
                "pandas64", new ResourcePandasScalarFunction("pandas64", 2, 64));
        util.addTemporarySystemFunction(
                "pandas32", new ResourcePandasScalarFunction("pandas32", 2, 32));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table "
                                + "SELECT pandas64(a, b), pandas32(a, b) FROM source_table");

        assertThat(pythonCalc.getParallelism()).isEqualTo(2);
        assertThat(pythonCalc.isParallelismConfigured()).isTrue();
        assertThat(getPythonOperatorConfig(pythonCalc))
                .returns(32, config -> config.get(PythonOptions.MAX_ARROW_BATCH_SIZE));
    }

    @Test
    void testUnspecifiedParallelismInheritsInput() throws Exception {
        util.getTableEnv().getConfig().set(PythonOptions.MAX_ARROW_BATCH_SIZE, 257);
        util.addTemporarySystemFunction(
                "pandas_default", new ResourcePandasScalarFunction("pandas_default", -1, -1));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table "
                                + "SELECT pandas_default(a, b), b FROM source_table");

        assertThat(pythonCalc.getParallelism()).isEqualTo(4);
        assertThat(pythonCalc.isParallelismConfigured()).isFalse();
        assertThat(getPythonOperatorConfig(pythonCalc))
                .returns(257, config -> config.get(PythonOptions.MAX_ARROW_BATCH_SIZE));
    }

    @Test
    void testAsyncConcurrencySetsConfiguredParallelism() {
        util.addTemporarySystemFunction(
                "async_func", new ResourceAsyncPythonScalarFunction("async_func", 3));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table SELECT async_func(a, b), b FROM source_table");

        assertThat(pythonCalc.getParallelism()).isEqualTo(3);
        assertThat(pythonCalc.isParallelismConfigured()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "python_parallel(a, b)",
                "python_parallel(a, b) + 1",
                "python_default(python_parallel(a, b), b)",
                "python_one(python_parallel(a, b), b)",
                "(python_row(a, b)).x"
            })
    void testSortLimitRemainsAfterExplicitConcurrencyProjection(String projection) {
        util.addTemporarySystemFunction(
                "python_parallel", new ResourcePandasScalarFunction("python_parallel", 2, -1));
        util.addTemporarySystemFunction(
                "python_default", new ResourcePandasScalarFunction("python_default", -1, -1));
        util.addTemporarySystemFunction(
                "python_one", new ResourcePandasScalarFunction("python_one", 1, -1));
        util.addTemporarySystemFunction(
                "python_row",
                new PythonScalarFunction(
                        "python_row",
                        new byte[0],
                        new DataType[] {DataTypes.INT(), DataTypes.INT()},
                        DataTypes.ROW(DataTypes.FIELD("x", DataTypes.INT())),
                        PythonFunctionKind.GENERAL,
                        true,
                        false,
                        false,
                        new PythonEnv(PythonEnv.ExecType.PROCESS),
                        2,
                        -1));

        final Transformation<?> transformation =
                getTransformation(
                        "INSERT INTO sink_table SELECT a, "
                                + projection
                                + " FROM source_table ORDER BY a DESC LIMIT 3 OFFSET 1");
        final OneInputTransformation<?, ?> sort =
                findOperator(transformation, SortLimitOperator.class::isInstance);
        assertThat(sort).isNotNull();
        assertThat(sort.getParallelism()).isEqualTo(1);
        OneInputTransformation<?, ?> pythonCalc = findPythonTransformation(sort);
        assertThat(pythonCalc).isNotNull();
        boolean hasParallelPythonCalc = false;
        while (pythonCalc != null) {
            assertThat(
                            findOperator(
                                    pythonCalc.getInputs().get(0),
                                    SortLimitOperator.class::isInstance))
                    .isNull();
            hasParallelPythonCalc |= pythonCalc.getParallelism() == 2;
            pythonCalc = findPythonTransformation(pythonCalc.getInputs().get(0));
        }
        assertThat(hasParallelPythonCalc).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 32})
    void testSortLimitPushesThroughProjectionWithoutExplicitConcurrency(int batchSize) {
        util.addTemporarySystemFunction(
                "python_default",
                new ResourcePandasScalarFunction("python_default", -1, batchSize));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table SELECT a, python_default(a, b) "
                                + "FROM source_table ORDER BY a LIMIT 3");

        assertThat(pythonCalc.getParallelism()).isEqualTo(1);
        assertThat(pythonCalc.isParallelismConfigured()).isFalse();
        assertThat(findOperator(pythonCalc.getInputs().get(0), SortLimitOperator.class::isInstance))
                .isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"LIMIT 3", "LIMIT 3 OFFSET 1"})
    void testLimitWithoutOrderingPushesThroughExplicitConcurrencyProjection(String limit) {
        util.addTemporarySystemFunction(
                "python_parallel", new ResourcePandasScalarFunction("python_parallel", 2, -1));

        final OneInputTransformation<?, ?> pythonCalc =
                getPythonTransformation(
                        "INSERT INTO sink_table SELECT a, python_parallel(a, b) "
                                + "FROM source_table "
                                + limit);

        assertThat(pythonCalc.getParallelism()).isEqualTo(2);
        assertThat(pythonCalc.isParallelismConfigured()).isTrue();
        assertThat(findOperator(pythonCalc.getInputs().get(0), LimitOperator.class::isInstance))
                .isNotNull();
    }

    private OneInputTransformation<?, ?> getPythonTransformation(String statement) {
        return findPythonTransformation(getTransformation(statement));
    }

    private Transformation<?> getTransformation(String statement) {
        final List<Operation> operations = util.getPlanner().getParser().parse(statement);
        final List<Transformation<?>> transformations =
                util.getPlanner()
                        .translate(Collections.singletonList((ModifyOperation) operations.get(0)));
        assertThat(transformations).hasSize(1);
        return transformations.get(0);
    }

    private OneInputTransformation<?, ?> findPythonTransformation(
            Transformation<?> transformation) {
        return findOperator(
                transformation, operator -> operator.getClass().getSimpleName().contains("Python"));
    }

    private OneInputTransformation<?, ?> findOperator(
            Transformation<?> transformation, Predicate<Object> predicate) {
        if (transformation instanceof OneInputTransformation) {
            final OneInputTransformation<?, ?> oneInput =
                    (OneInputTransformation<?, ?>) transformation;
            if (oneInput.getOperatorFactory() instanceof SimpleOperatorFactory) {
                final Object operator =
                        ((SimpleOperatorFactory<?>) oneInput.getOperatorFactory()).getOperator();
                if (predicate.test(operator)) {
                    return oneInput;
                }
            }
        }
        for (Transformation<?> input : transformation.getInputs()) {
            final OneInputTransformation<?, ?> result = findOperator(input, predicate);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    private Configuration getPythonOperatorConfig(OneInputTransformation<?, ?> transformation)
            throws Exception {
        final Object operator =
                ((SimpleOperatorFactory<?>) transformation.getOperatorFactory()).getOperator();
        Class<?> operatorClass = operator.getClass();
        while (operatorClass != null) {
            try {
                final Field configField = operatorClass.getDeclaredField("config");
                configField.setAccessible(true);
                return (Configuration) configField.get(operator);
            } catch (NoSuchFieldException ignored) {
                operatorClass = operatorClass.getSuperclass();
            }
        }
        throw new AssertionError("Python operator configuration field was not found.");
    }

    public static class ResourcePandasScalarFunction extends PandasScalarFunction {
        private final int parallelism;
        private final int maxArrowBatchSize;

        public ResourcePandasScalarFunction(String name, int parallelism, int maxArrowBatchSize) {
            super(name);
            this.parallelism = parallelism;
            this.maxArrowBatchSize = maxArrowBatchSize;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        @Override
        public int getMaxArrowBatchSize() {
            return maxArrowBatchSize;
        }
    }

    public static class ResourceAsyncPythonScalarFunction extends AsyncPythonScalarFunction {
        private final int parallelism;

        public ResourceAsyncPythonScalarFunction(String name, int parallelism) {
            super(name);
            this.parallelism = parallelism;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }
    }
}
